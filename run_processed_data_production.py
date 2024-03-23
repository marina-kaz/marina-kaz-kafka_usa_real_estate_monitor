from producers.producers import RawDataConsumer, ProcessedDataProducer
from producers.configurations import (data_producer_topic_config, 
                                      data_processor_config, 
                                      data_processor_topic_config)


if __name__ == "__main__":
    consumer = RawDataConsumer(data_producer_topic_config=data_producer_topic_config)
    producer = ProcessedDataProducer(consumer=consumer, data_processor_config=data_processor_config)
    producer.produce(data_processor_topic_config=data_processor_topic_config)
