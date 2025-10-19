# db_consumer.py
# Reads Kafka "metrics" and inserts into Postgres rtm.metrics_raw
import os, json, signal
from time import time, sleep
from typing import List, Tuple
from confluent_kafka import Consumer, KafkaException
import psycopg2, psycopg2.extras, psycopg2.errors

# Config from env (host-local defaults)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "metrics")
KAFKA_GROUP     = os.getenv("KAFKA_GROUP", "metrics-db-writer")

PG_DSN = os.getenv("PG_DSN", "dbname=rtm user=rtm password=rtm host=localhost port=5432")

POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "1.0"))
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "100"))
BATCH_SEC    = float(os.getenv("BATCH_SEC", "2.0"))

running = True
def _handle(sig, frame):
    global running
    running = False
signal.signal(signal.SIGINT, _handle)
signal.signal(signal.SIGTERM, _handle)

def get_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })

def get_pg_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    return conn

def insert_batch(cur, rows):
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO metrics_raw(
             host, cpu, memory, ts,
             load1, load5, load15,
             disk_used_gb, disk_used_pct,
             net_rx_kbps, net_tx_kbps, os, arch
           ) VALUES %s""",
        [(
            d.get("host"),
            float(d.get("cpu")),
            float(d.get("memory")),
            psycopg2.TimestampFromTicks(float(d.get("timestamp"))),
            float(d.get("load1", 0.0)),
            float(d.get("load5", 0.0)),
            float(d.get("load15", 0.0)),
            float(d.get("disk_used_gb", 0.0)),
            float(d.get("disk_used_pct", 0.0)),
            float(d.get("net_rx_kbps", 0.0)),
            float(d.get("net_tx_kbps", 0.0)),
            d.get("os", None),
            d.get("arch", None),
          )
          for d in rows
        ],
        page_size=len(rows)
    )

def main():
    print(f"Kafka: {KAFKA_BOOTSTRAP} | topic: {KAFKA_TOPIC} | group: {KAFKA_GROUP}")
    print(f"Postgres DSN: {PG_DSN}")

    consumer = get_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    conn = get_pg_conn()
    cur  = conn.cursor()

    batch, last_flush = [], time()

    try:
        while running:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                # time-based flush
                if batch and (time() - last_flush >= BATCH_SEC):
                    insert_batch(cur, batch)
                    conn.commit()
                    consumer.commit(asynchronous=False)
                    print(f"Committed batch of {len(batch)}")
                    batch.clear()
                    last_flush = time()
                continue

            if msg.error():
                print(f"[Kafka] {msg.error()}")
                continue

            try:
                data = json.loads(msg.value())
                batch.append(data)
            except Exception as e:
                print(f"[Parse] Skipping msg at {msg.topic()}[{msg.partition()}]@{msg.offset()}: {e}")

            # size-based flush
            if len(batch) >= BATCH_SIZE:
                insert_batch(cur, batch)
                conn.commit()
                consumer.commit(asynchronous=False)
                print(f"Committed batch of {len(batch)}")
                batch.clear()
                last_flush = time()

        # graceful shutdown flush
        if batch:
            insert_batch(cur, batch)
            conn.commit()
            consumer.commit(asynchronous=False)
            print(f"Committed final batch of {len(batch)}")

    except KafkaException as ke:
        print(f"[Kafka] Exception: {ke}")
    except psycopg2.Error as pe:
        print(f"[Postgres] Exception: {pe}")
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
        consumer.close()
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
