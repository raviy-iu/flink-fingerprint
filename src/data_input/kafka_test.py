from json import dumps
import pandas as pd
from kafka import KafkaProducer


producer_ = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"))
start_time = int(pd.to_datetime("2026-01-27 06:30:00").timestamp() * 1000)
end_time = int(pd.to_datetime("2026-01-27 07:30:00").timestamp() * 1000)

equipment_requests = [
    {"equipmentName": "SCL_LINE_2_KILN", "equipmentId": 0},
]

for req in equipment_requests:
    message = {
        "plantId": 2618,
        "equipmentName": req["equipmentName"],
        "startTime": start_time,
        "endTime": end_time,
        "equipmentId": req["equipmentId"],
        "plantName": "Star-Cement",
    }
    producer_.send("equipment_process_odr_request", value=message)

producer_.flush()
