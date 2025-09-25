import argparse
import json
import threading
import time
import uuid
from typing import List, Optional
from kafka import KafkaConsumer

def make_consumer(
    topic: str,
    group_id: str,
    brokers: List[str],
    client_id: Optional[str] = None,
    from_beginning: bool = False,
    enable_auto_commit: bool = True,
    **extra,
) -> KafkaConsumer:

    return KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        group_id=group_id,
        client_id=client_id,
        auto_offset_reset="earliest" if from_beginning else "latest",
        enable_auto_commit=enable_auto_commit,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        **extra,
    )

class ConsumerWorker(threading.Thread):


    def __init__(self, name: str, topic: str, group_id: str, brokers: List[str], from_beginning: bool):
        super().__init__(name=name, daemon=True)
        self.topic = topic
        self.group_id = group_id
        self.brokers = brokers
        self.from_beginning = from_beginning
        self._stop = threading.Event()
        self._consumer: Optional[KafkaConsumer] = None

    def run(self):
        self._consumer = make_consumer(
            topic=self.topic,
            group_id=self.group_id,
            brokers=self.brokers,
            client_id=self.name,
            from_beginning=self.from_beginning,
        )
        print(f"[{self.name}] started | group={self.group_id} | topic={self.topic}")
        try:
            for msg in self._consumer:
                if self._stop.is_set():
                    break
                print(
                    f"[{self.name}] partition={msg.partition} offset={msg.offset} "
                    f"key={msg.key} value={msg.value}"
                )
        except Exception as e:
            print(f"[{self.name}] erro: {e}")
        finally:
            try:
                if self._consumer:
                    self._consumer.close()
            except Exception:
                pass
            print(f"[{self.name}] stopped")

    def stop(self):
        self._stop.set()

def main():
    parser = argparse.ArgumentParser(description="Spawner de múltiplos consumidores Kafka")
    parser.add_argument("--topic", default="pedidos")
    parser.add_argument("--brokers", default="localhost:9094", help="lista separada por vírgula")
    parser.add_argument("--instances", type=int, default=2, help="quantos consumidores criar")
    parser.add_argument("--group", default="servico-generico", help="nome base do group_id")
    parser.add_argument("--mode", choices=["broadcast", "workshare"], default="broadcast",
                        help="broadcast: grupos diferentes; workshare: mesmo grupo")
    parser.add_argument("--from-beginning", action="store_true", help="começar do início se não houver commit")
    args = parser.parse_args()

    brokers = [b.strip() for b in args.brokers.split(",") if b.strip()]

    workers: List[ConsumerWorker] = []
    for i in range(args.instances):
        if args.mode == "broadcast":

            group_id = f"{args.group}-{i+1}-{uuid.uuid4().hex[:6]}"
        else:

            group_id = args.group

        name = f"consumer-{i+1}"
        w = ConsumerWorker(
            name=name,
            topic=args.topic,
            group_id=group_id,
            brokers=brokers,
            from_beginning=args.from_beginning,
        )
        w.start()
        workers.append(w)

    print(f"↑ Subidos {len(workers)} consumidores. Pressione Ctrl+C para encerrar.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nEncerrando consumidores...")
        for w in workers:
            w.stop()

        time.sleep(1)

if __name__ == "__main__":
    main()
