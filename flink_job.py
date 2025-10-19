import json, os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, WatermarkStrategy, Configuration, Row

# NEW imports for Table API
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.table.expressions import col, lit, call

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "metrics")

PG_URL  = os.getenv("PG_URL",  "jdbc:postgresql://localhost:5432/rtm")
PG_USER = os.getenv("PG_USER", "rtm")
PG_PASS = os.getenv("PG_PASS", "rtm")

ALPHA = float(os.getenv("EWMA_ALPHA", "0.2"))

def parse_json(value):
    try:
        d = json.loads(value)
        return (d.get("host","unknown"),
                float(d.get("cpu",0.0)),
                float(d.get("memory",0.0)),
                int(d.get("timestamp",0)))
    except Exception:
        return None

def main():
    import os, pathlib
    conf = Configuration()

    jars_env = os.getenv("FLINK_JARS")
    if jars_env:
        conf.set_string("pipeline.jars", jars_env)
    else:
        jars_dir = pathlib.Path(__file__).parent / "jars"
        if jars_dir.exists():
            jar_urls = [
                "file://" + str((jars_dir / name).resolve())
                for name in os.listdir(jars_dir) if name.endswith(".jar")
            ]
            if jar_urls:
                conf.set_string("pipeline.jars", ";".join(jar_urls))

    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    source = KafkaSource.builder()\
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)\
        .set_topics(KAFKA_TOPIC)\
        .set_group_id("flink-ewma")\
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())\
        .set_value_only_deserializer(SimpleStringSchema())\
        .build()

    ds = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "metrics-kafka"
    )

    parsed = ds.map(
        parse_json,
        output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()])
    ).filter(lambda x: x is not None)

    def ewma_anomaly():
        state = {}
        def fn(t):
            host, cpu, mem, ts = t
            value = cpu
            prev = state.get(host)
            if prev is None:
                state[host] = (value, value * value)
                return None
            ewma, ewmsq = prev
            var_prev = max(ewmsq - ewma*ewma, 0.0)
            std_prev = var_prev ** 0.5
            score    = abs(value - ewma) / (std_prev if std_prev > 1e-9 else 1.0)
            ewma_new  = ALPHA*value + (1-ALPHA)*ewma
            ewmsq_new = ALPHA*(value*value) + (1-ALPHA)*ewmsq
            state[host] = (ewma_new, ewmsq_new)
            if score >= 3.0:
                row = Row(host, ts, value, mem, "cpu", float(score), "EWMA+3σ")
                row.set_field_names(["host","ts","cpu","memory","metric","score","rule"])
                return row
            return None
        return fn

    anomalies = parsed.map(
        ewma_anomaly(),
        output_type=Types.ROW_NAMED(
            ["host","ts","cpu","memory","metric","score","rule"],
            [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(),
             Types.STRING(), Types.DOUBLE(), Types.STRING()]
        )
    ).filter(lambda x: x is not None)

    # --- Table API JDBC sink (NEW) ---
    t_env.execute_sql(f"""
    CREATE TABLE anomalies_sink (
      host STRING,
      ts BIGINT,
      cpu DOUBLE,
      memory DOUBLE,
      metric STRING,
      score DOUBLE,
      rule STRING
    ) WITH (
      'connector' = 'jdbc',
      'url' = '{PG_URL}',
      'table-name' = 'metrics_anomalies',
      'driver' = 'org.postgresql.Driver',
      'username' = '{PG_USER}',
      'password' = '{PG_PASS}'
    )
    """)

    t = t_env.from_data_stream(
        anomalies,
        Schema.new_builder()
          .column("host", DataTypes.STRING())
          .column("ts", DataTypes.BIGINT())
          .column("cpu", DataTypes.DOUBLE())
          .column("memory", DataTypes.DOUBLE())
          .column("metric", DataTypes.STRING())
          .column("score", DataTypes.DOUBLE())
          .column("rule", DataTypes.STRING())
          .build()
    ).select(
        col("host"),
        col("ts"),
        col("cpu"),
        col("memory"),
        col("metric"),
        col("score"),
        col("rule")
    )

    t.execute_insert("anomalies_sink").wait()

    env.execute("rtm-ewma-anomaly")

if __name__ == "__main__":
    main()
