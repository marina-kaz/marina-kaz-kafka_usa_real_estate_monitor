import json
import random
import time

from confluent_kafka import Producer, Consumer
from producers.configurations import data_producer_config, data_producer_topic_config
from utils import load_data
from model.model import Predictor
from utils import Sample


class RawDataProducer():

    def __init__(self, data_producer_config: dict) -> None:
        self.producer = Producer(data_producer_config)
        self.data = load_data()

    def produce(self, data_producer_topic_config: dict) -> None:
        while True:
            sample_id = random.randint(self.data.index.min(), self.data.index.max())
            sample = self.data.iloc[sample_id]
            self.producer.produce(data_producer_topic_config, 
                                  key='1', 
                                  value=json.dumps({'sample_id': sample_id, **sample.to_dict()}))
            self.producer.flush()
            print(f'Produced raw sample: {sample_id}')
            time.sleep(random.randint(1, 10))


class RawDataConsumer:

    def __init__(self, data_producer_topic_config: dict) -> None:
        topic_consume = [data_producer_topic_config]
        conf_consume = {**data_producer_config, 'group.id': 'data_processors'}
        self.consumer = Consumer(conf_consume)
        self.consumer.subscribe(topic_consume)


class ProcessedDataProducer:

    def __init__(self, consumer: RawDataConsumer, data_processor_config: dict) -> None:
        self.consumer = consumer
        self.producer = Producer(data_processor_config)
        self.predictor = Predictor()

    def produce(self, data_processor_topic_config: dict) -> None:
        while True:
            msg = self.consumer.consumer.poll(timeout=1000)

            if msg is not None:
                data = json.loads(msg.value().decode('utf-8'))
                sample = Sample(bed=data['bed'], bath=data['bath'], house_size=data['house_size'])
                prediction = self.predictor.predict(sample)

                result_data = {'prediction': prediction, **data}

                self.producer.produce(data_processor_topic_config, key='1', value=json.dumps(result_data))
                self.producer.flush()

                sample_id = data['sample_id']
                print(f'Processed: {sample_id}', flush=True)
