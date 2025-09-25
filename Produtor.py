import json
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKERS = ['localhost: 9094']
TOPIC = "pedidos"

def json_serializer(data: dict) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode('utf-8')

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=json_serializer,
        acks='all',
        linger_ms=5,
        retries=5
    )

    print(f"enviando mensagem para {TOPIC} Aperte Ctrl + C para cancelar!")
    i = 1
    try:
        while True:
            pedido = {
                "order_id": str(uuid.uuid4()),
                "customer_id": f"CUST-{i:04d}",
                "total": round(50 + i * 1.23, 2),
                "items":[
                    {"sku": "ABC-123", "qty":1},
                    {"sku": "XYZ-999", "qty":(i%3) + 1},
                ],
                "created_at": datetime.now(timezone.utc).isoformat()
            }

            key =  pedido["customer_id"].encode('utf-8')

            future = producer.send(TOPIC, value=pedido, key=key)
            metadata = future.get(timeout=10)

            print(f"[PRODUTOR] Enviando order_id={pedido["order_id"]}"  f"para partition={metadata.partition}, offset={metadata.offset}")

            i += 1
            time.sleep(0.8)

    except KeyboardInterrupt:
        pass
    finally:
        print("Flush/close producer...")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()