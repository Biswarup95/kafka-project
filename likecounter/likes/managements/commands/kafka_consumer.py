from django.core.management.base import BaseCommand
from confluent_kafka import Consumer
import os
import json
from collections import defaultdict

class Command(BaseCommand):
    help = "Run kafka consumer"
    def process_batch(like_batch):
        print(like_batch)
    
    def handle(self, *args, **options):
        like_batch = defaultdict(int)

        conf = {
            'bootstrap.servers': os.getenv('KAFKA_BROKER_URL', 'localhost:9092'),
            'group.id': 'location_group',
            'auto.offset.reset': 'earliest'
            }
            
        consumer = Consumer(conf)
        consumer.subscribe(['like_topic'])
        total_messages = 0
        try:
            while True:
                msg = consumer.poll(timeout = 1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(msg.error())
                    continue

                data = json.loads(msg.value().decode('utf-8'))
                post_id = data['post_id']
                like_batch[post_id] += 1
                total_messages += 1
                if total_messages >= 1000:
                    self.process_batch(like_batch)
                    like_batch.clear()
                    total_messages = 0
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
