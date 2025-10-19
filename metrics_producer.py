# metrics_producer.py — REAL host metrics via psutil
import os, time, json, signal, socket, platform
from typing import Tuple
from confluent_kafka import Producer
import psutil

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "metrics")
INTERVAL_SEC = float(os.getenv("INTERVAL_SEC", "2"))  # sample every 2s by default

p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

stop = False
def _stop(sig, frame):
    global stop
    stop = True
signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

host = socket.gethostname() or platform.node() or "unknown-host"

# prime network counters for rate calc
prev_net = psutil.net_io_counters()
prev_time = time.time()

def _rates(prev: Tuple[float, float], cur: Tuple[float, float], dt: float):
    if dt <= 0: return (0.0, 0.0)
    rx_rate_kbps = (cur[0] - prev[0]) * 8.0 / 1000.0 / dt  # bytes -> kilobits / s
    tx_rate_kbps = (cur[1] - prev[1]) * 8.0 / 1000.0 / dt
    return (max(rx_rate_kbps, 0.0), max(tx_rate_kbps, 0.0))

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

print(f"Producing REAL metrics every {INTERVAL_SEC}s to '{KAFKA_TOPIC}' via {KAFKA_BOOTSTRAP} (Ctrl+C to stop)")
# first call to cpu_percent initializes stats; use interval=None then actual interval in loop
psutil.cpu_percent(interval=None)

while not stop:
    start = time.time()

    cpu_total = psutil.cpu_percent(interval=None)  # % total cpu
    mem = psutil.virtual_memory()
    # keep DB schema compatible: memory in MB just like earlier “random” scale (approx)
    memory_mb = round(mem.used / (1024 * 1024), 3)

    load1, load5, load15 = (0.0, 0.0, 0.0)
    try:
        load1, load5, load15 = psutil.getloadavg()  # macOS supported
    except (AttributeError, OSError):
        pass

    disk = psutil.disk_usage("/")
    disk_used_gb = round(disk.used / (1024**3), 3)
    disk_used_pct = float(disk.percent)

    cur_net = psutil.net_io_counters()
    now = time.time()
    rx_kbps, tx_kbps = _rates((prev_net.bytes_recv, prev_net.bytes_sent),(cur_net.bytes_recv, cur_net.bytes_sent),now - prev_time)
    prev_net, prev_time = cur_net, now

    payload = {
        "host": host,
        "cpu": float(cpu_total),                # % total CPU
        "memory": float(memory_mb),             # MB used
        "timestamp": int(now),                  # epoch seconds
        # extra (optional) fields for future expansion
        "load1": float(load1),
        "load5": float(load5),
        "load15": float(load15),
        "disk_used_gb": float(disk_used_gb),
        "disk_used_pct": float(disk_used_pct),
        "net_rx_kbps": float(rx_kbps),
        "net_tx_kbps": float(tx_kbps),
        "os": platform.system(),
        "arch": platform.machine(),
    }

    p.produce(KAFKA_TOPIC, json.dumps(payload).encode("utf-8"), callback=delivery_report)
    p.poll(0)

    # sleep the remainder of INTERVAL_SEC
    elapsed = time.time() - start
    if elapsed < INTERVAL_SEC:
        time.sleep(INTERVAL_SEC - elapsed)

print("\nFlushing…")
p.flush(5)
print("Done.")