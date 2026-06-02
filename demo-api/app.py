import json
import os
from datetime import datetime
from datetime import timezone
from urllib.error import URLError
from urllib.request import urlopen

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from kafka import KafkaAdminClient
from kafka import KafkaProducer
from psycopg2.extras import RealDictCursor

from anomaly_explanations import enrich_anomalies
from anomaly_explanations import summarize_current_metric
from incident_commands import build_incident_command
from pipeline_status import age_seconds
from pipeline_status import component
from pipeline_status import describe_age
from pipeline_status import serialize_datetime
from pipeline_status import status_rollup
from rate_limit import IncidentRateLimiter


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INCIDENT_TOPIC = os.getenv("INCIDENT_COMMANDS_TOPIC", "incident_commands")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
PUBLIC_GRAFANA_URL = os.getenv("PUBLIC_GRAFANA_URL", "http://localhost:3000").strip()
FLINK_HEALTH_URL = os.getenv("FLINK_HEALTH_URL", "http://stream-processor:8090/health")
FRESH_METRIC_SECONDS = int(os.getenv("FRESH_METRIC_SECONDS", "60"))
INCIDENT_MIN_INTERVAL_SECONDS = float(os.getenv("INCIDENT_MIN_INTERVAL_SECONDS", "3"))
INCIDENT_RATE_LIMIT_WINDOW_SECONDS = float(os.getenv("INCIDENT_RATE_LIMIT_WINDOW_SECONDS", "60"))
INCIDENT_RATE_LIMIT_MAX_REQUESTS = int(os.getenv("INCIDENT_RATE_LIMIT_MAX_REQUESTS", "12"))
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("PG_DB", "rtm")
PG_USER = os.getenv("PG_USER", "rtm")
PG_PASSWORD = os.getenv("PG_PASSWORD", "rtm")
REQUIRED_TOPICS = ("metrics_raw", "metrics_anomalies", INCIDENT_TOPIC)
REQUIRED_SUBJECTS = (
    "metrics_raw-value",
    "metrics_anomalies-value",
    "incident_commands-value",
)
REQUIRED_HYPERTABLES = ("metrics_raw", "metrics_anomalies")

app = FastAPI(title="RTM-AI Demo API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

producer = None
incident_rate_limiter = IncidentRateLimiter(
    min_interval_seconds=INCIDENT_MIN_INTERVAL_SECONDS,
    window_seconds=INCIDENT_RATE_LIMIT_WINDOW_SECONDS,
    max_requests=INCIDENT_RATE_LIMIT_MAX_REQUESTS,
)


def get_db_connection():
    return psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            request_timeout_ms=30000,
            api_version_auto_timeout_ms=30000,
        )
    return producer


def kafka_is_available():
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=10000,
    )
    try:
        admin.list_topics()
        return True
    finally:
        admin.close()


def short_error(error):
    return str(error).splitlines()[0][:160]


def get_json_url(url, timeout=2):
    with urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def query_rows(sql, params):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]
    except (psycopg2.errors.UndefinedColumn, psycopg2.errors.UndefinedTable):
        return []


def check_api_component():
    return component("API", "ok", "serving requests")


def check_kafka_component():
    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=10000,
        )
        topics = set(admin.list_topics())
        topic_status = {topic: topic in topics for topic in REQUIRED_TOPICS}
        missing = [topic for topic, exists in topic_status.items() if not exists]
        status = "degraded" if missing else "ok"
        details = "topics ready" if not missing else f"missing {', '.join(missing)}"
        return component("Kafka", status, details, topics=topic_status)
    except Exception as e:
        return component("Kafka", "degraded", short_error(e), topics={})
    finally:
        if admin:
            admin.close()


def check_schema_registry_component():
    try:
        subjects = set(get_json_url(f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects"))
        subject_status = {subject: subject in subjects for subject in REQUIRED_SUBJECTS}
        missing = [subject for subject, exists in subject_status.items() if not exists]
        status = "degraded" if missing else "ok"
        details = "schemas registered" if not missing else f"missing {len(missing)} schema subjects"
        return component("Schema Registry", status, details, subjects=subject_status)
    except (OSError, URLError, ValueError) as e:
        return component("Schema Registry", "degraded", short_error(e), subjects={})


def check_flink_component():
    try:
        data = get_json_url(FLINK_HEALTH_URL)
        status = "ok" if data.get("status") == "ok" else "degraded"
        details = data.get("job") or "health endpoint responded"
        return component("Flink", status, details, processor=data)
    except (OSError, URLError, ValueError) as e:
        return component("Flink", "degraded", short_error(e), processor={})


def check_grafana_component():
    try:
        data = get_json_url(f"{GRAFANA_URL.rstrip('/')}/api/health")
        status = "ok" if data.get("database") == "ok" else "degraded"
        details = f"Grafana {data.get('version', 'unknown')}"
        return component("Grafana", status, details, grafana=data)
    except (OSError, URLError, ValueError) as e:
        return component("Grafana", "degraded", short_error(e), grafana={})


def fetch_storage_status(now=None):
    now = now or datetime.now(timezone.utc)
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT hypertable_name
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = current_schema()
                """
            )
            hypertables = {row["hypertable_name"] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT COUNT(*) AS row_count, MAX(time) AS latest_at
                FROM metrics_raw
                WHERE event_id IS NOT NULL AND time IS NOT NULL
                """
            )
            metrics = dict(cur.fetchone())

            cur.execute(
                """
                SELECT COUNT(*) AS row_count, MAX(time) AS latest_at
                FROM metrics_anomalies
                WHERE event_id IS NOT NULL AND time IS NOT NULL
                """
            )
            anomalies = dict(cur.fetchone())

    latest_metric_at = metrics["latest_at"]
    latest_anomaly_at = anomalies["latest_at"]
    return {
        "hypertables": {table: table in hypertables for table in REQUIRED_HYPERTABLES},
        "metrics_rows": metrics["row_count"],
        "anomaly_rows": anomalies["row_count"],
        "latest_metric_at": serialize_datetime(latest_metric_at),
        "latest_anomaly_at": serialize_datetime(latest_anomaly_at),
        "latest_metric_age_seconds": age_seconds(latest_metric_at, now),
        "latest_anomaly_age_seconds": age_seconds(latest_anomaly_at, now),
    }


def build_timescale_component(storage):
    missing = [table for table, exists in storage["hypertables"].items() if not exists]
    status = "degraded" if missing else "ok"
    details = "hypertables ready" if not missing else f"missing hypertables: {', '.join(missing)}"
    return component("TimescaleDB", status, details, storage=storage)


def build_telemetry_component(storage):
    metric_age = storage["latest_metric_age_seconds"]
    if metric_age is None:
        return component("Telemetry", "degraded", "no metric events stored", freshness=storage)
    if metric_age > FRESH_METRIC_SECONDS:
        return component(
            "Telemetry",
            "degraded",
            f"latest metric {describe_age(metric_age)}",
            freshness=storage,
        )
    anomaly_age = describe_age(storage["latest_anomaly_age_seconds"])
    return component(
        "Telemetry",
        "ok",
        f"latest metric {describe_age(metric_age)}; anomaly {anomaly_age}",
        freshness=storage,
    )


def check_timescale_and_telemetry_components():
    try:
        storage = fetch_storage_status()
        return build_timescale_component(storage), build_telemetry_component(storage)
    except Exception as e:
        degraded_storage = component("TimescaleDB", "degraded", short_error(e), storage={})
        degraded_telemetry = component("Telemetry", "degraded", "storage unavailable", freshness={})
        return degraded_storage, degraded_telemetry


def fetch_latest_metrics(limit):
    return query_rows(
        """
        SELECT event_id, time, host, ts, cpu, mem, disk, net_in, net_out, scenario, source
        FROM metrics_raw
        WHERE event_id IS NOT NULL AND time IS NOT NULL
        ORDER BY time DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )


def fetch_latest_anomalies(limit):
    return query_rows(
        """
        SELECT event_id, time, host, ts, metric, value, baseline, std, score, severity, scenario
        FROM metrics_anomalies
        WHERE event_id IS NOT NULL AND time IS NOT NULL
        ORDER BY time DESC NULLS LAST
        LIMIT %s
        """,
        (limit,),
    )


def fetch_recent_anomalies(limit, window_minutes=10):
    return query_rows(
        """
        SELECT event_id, time, host, ts, metric, value, baseline, std, score, severity, scenario
        FROM metrics_anomalies
        WHERE event_id IS NOT NULL
          AND time IS NOT NULL
          AND time >= NOW() - (%s * INTERVAL '1 minute')
        ORDER BY time DESC NULLS LAST
        LIMIT %s
        """,
        (window_minutes, limit),
    )


@app.get("/api/health")
def health():
    postgres_ok = False
    kafka_ok = False

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        postgres_ok = True
    except Exception:
        postgres_ok = False

    try:
        kafka_ok = kafka_is_available()
    except Exception:
        kafka_ok = False

    return {
        "status": "ok" if postgres_ok and kafka_ok else "degraded",
        "postgres": postgres_ok,
        "kafka": kafka_ok,
    }


@app.get("/api/pipeline/status")
def pipeline_status():
    timescale_component, telemetry_component = check_timescale_and_telemetry_components()
    components = {
        "api": check_api_component(),
        "kafka": check_kafka_component(),
        "schema_registry": check_schema_registry_component(),
        "flink": check_flink_component(),
        "timescale": timescale_component,
        "telemetry": telemetry_component,
        "grafana": check_grafana_component(),
    }
    return {
        "status": status_rollup(components),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "components": components,
    }


@app.get("/api/config")
def config():
    return {
        "grafana_url": PUBLIC_GRAFANA_URL or None,
        "incident_cooldown_seconds": INCIDENT_MIN_INTERVAL_SECONDS,
    }


@app.get("/api/metrics/latest")
def latest_metrics(limit: int = 50):
    limit = max(1, min(limit, 500))
    return fetch_latest_metrics(limit)


@app.get("/api/anomalies/latest")
def latest_anomalies(limit: int = 50):
    limit = max(1, min(limit, 500))
    return enrich_anomalies(fetch_latest_anomalies(limit))


@app.get("/api/summary")
def summary():
    metrics = fetch_latest_metrics(1)
    anomalies = enrich_anomalies(fetch_recent_anomalies(5))
    latest_metric = metrics[0] if metrics else None
    return {
        "current": summarize_current_metric(latest_metric),
        "latest_metric": latest_metric,
        "latest_anomalies": anomalies,
    }


@app.post("/api/incidents")
def create_incident(payload: dict):
    try:
        command = build_incident_command(payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    allowed, retry_after = incident_rate_limiter.check()
    if not allowed:
        retry_label = f"{retry_after}s" if retry_after < 60 else f"{retry_after // 60}m"
        raise HTTPException(
            status_code=429,
            detail=f"Please wait before starting another simulation. Try again in {retry_label}.",
            headers={"Retry-After": str(retry_after)},
        )

    try:
        get_producer().send(INCIDENT_TOPIC, command).get(timeout=10)
        return command
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to publish incident: {e}") from e


app.mount("/", StaticFiles(directory="static", html=True), name="static")
