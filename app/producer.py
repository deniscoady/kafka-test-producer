import os
import uuid
import signal
import time
import sys
import math

from tqdm import tqdm
from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM    = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
KAFKA_SASL_USERNAME     = os.environ["KAFKA_SASL_USERNAME"]
KAFKA_SASL_PASSWORD     = os.environ["KAFKA_SASL_PASSWORD"]
JOB_TOPIC_NAME          = os.environ["JOB_TOPIC_NAME"]
JOB_MSG_BYTES           = int(os.environ["JOB_MSG_BYTES"])
JOB_MAX_BYTES           = int(os.environ["JOB_MAX_BYTES"])
JOB_LINGER_MS           = int(os.environ.get("JOB_LINGER_MS", 100))
JOB_BATCH_SIZE          = int(os.environ.get("JOB_BATCH_SIZE", 100))

MESSAGE = "0" * JOB_MSG_BYTES

def _fmt_bytes(n):
  """Human-friendly bytes."""
  for unit in ("B", "KB", "MB", "GB", "TB"):
    if n < 1024.0:
      return f"{n:,.3f} {unit}"
    n /= 1024.0
  return f"{n:,.3f} PB"

def _fmt_rate(bps):
  """Human-friendly bytes/sec."""
  for unit in ("B/s", "KB/s", "MB/s", "GB/s", "TB/s"):
    if bps < 1024.0:
      return f"{bps:,.2f} {unit}"
    bps /= 1024.0
  return f"{bps:,.2f} PB/s"

def main():

  print(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
  print(f"KAFKA_SECURITY_PROTOCOL: {KAFKA_SECURITY_PROTOCOL}")

  print(f"KAFKA_SASL_MECHANISM   : {KAFKA_SASL_MECHANISM}")
  print(f"KAFKA_SASL_USERNAME    : {KAFKA_SASL_USERNAME}")
  print(f"KAFKA_SASL_PASSWORD    : {KAFKA_SASL_PASSWORD}")

  print(f"JOB_TOPIC_NAME: {JOB_TOPIC_NAME}")
  print(f"JOB_MSG_BYTES : {JOB_MSG_BYTES}")
  print(f"JOB_MAX_BYTES : {JOB_MAX_BYTES}")
  print(f"JOB_BATCH_SIZE: {JOB_BATCH_SIZE}")

  # Tuned for throughput: big batches, short acks, lightweight callbacks
  producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": KAFKA_SECURITY_PROTOCOL,
    "sasl.mechanisms":   KAFKA_SASL_MECHANISM,
    "sasl.username":     KAFKA_SASL_USERNAME,
    "sasl.password":     "******" * (len(KAFKA_SASL_PASSWORD) > 0),
    #
    "compression.type": "none",
    "linger.ms": JOB_LINGER_MS,
    "batch.size": JOB_MSG_BYTES * JOB_BATCH_SIZE,
    "queue.buffering.max.kbytes": JOB_MSG_BYTES * JOB_BATCH_SIZE / 1024,
    "acks": 1,
    "enable.idempotence": False,
    "socket.keepalive.enable": True,
  })

  running = True
  bytes_sent = 0

  def on_delivery_cb(err, msg):
    if err is not None:
      # Print errors so the user can see any broker/backpressure issues.
      sys.stderr.write(f"Delivery failed for offset={msg.offset()} error={err}\n")

  def send(payload, topic):
    while True:
      try:
        key = str(uuid.uuid4())
        producer.produce(topic, key=key, value=payload, on_delivery=on_delivery_cb)
        break
      except BufferError:
        # Local queue full — give librdkafka time to transmit & invoke callbacks
        producer.poll(0.1)  # 100ms backoff

  # Compute accurate batch counts
  total_messages = math.ceil(JOB_MAX_BYTES / JOB_MSG_BYTES)
  batches = math.ceil(total_messages / JOB_BATCH_SIZE)

  last_batch_rate = 0.0
  start = time.time()
  with tqdm(total=batches, unit="batch", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} {unit} {postfix}") as t:
    for _ in range(batches):
      if not running:
        break

      producer.poll(0)
      # start = time.time()

      # Send one batch
      for _ in range(JOB_BATCH_SIZE):
        send(MESSAGE, JOB_TOPIC_NAME)
        bytes_sent += JOB_MSG_BYTES

      # Let callbacks run; tiny poll to avoid tight loop starvation
      producer.poll(0)

      # Compute average throughput so far
      elapsed = max(time.time() - start, 1e-9)  # avoid /0
      rate = bytes_sent / elapsed  # bytes per second

      # Update progress bar postfix
      t.set_postfix({
        "sent": _fmt_bytes(bytes_sent),
        "tput": _fmt_rate(rate),
      })
      t.update(1)

  # Ensure all messages are sent before exiting
  producer.flush()
  print(f"Total bytes sent: {bytes_sent} ({_fmt_bytes(bytes_sent)})")
  print(f"Last-batch throughput: {_fmt_rate(last_batch_rate)}")

if __name__ == "__main__":
  main()

