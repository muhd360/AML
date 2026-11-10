import csv
import json
import os
import time
from confluent_kafka import Producer

NS = "aml-eventhubs"  # replace with your Event Hubs namespace name
p = Producer({
    "bootstrap.servers": f"{NS}.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": os.environ["EH_SEND_CONN"],  # never hard-code it
})

with open("HI-Small_Trans.csv") as f:
    reader = csv.reader(f)
    header = next(reader)
    # CSV has two columns both named "Account" — read by position
    for i, r in enumerate(reader):
        msg = {
            "txn_id":             f"T{i:08d}",
            "ts":                 r[0],
            "from_bank":          r[1],
            "from_account":       r[2],
            "to_bank":            r[3],
            "to_account":         r[4],
            "amount_received":    r[5],
            "receiving_currency": r[6],
            "amount_paid":        r[7],
            "payment_currency":   r[8],
            "payment_format":     r[9],
            "is_laundering":      r[10],
        }
        p.produce("transactions", key=r[2], value=json.dumps(msg))
        if i % 5000 == 0:
            p.flush()
            time.sleep(1)  # throttle: ~5,000 events/second

msg_count = i + 1
p.flush()
print("sent", msg_count)
