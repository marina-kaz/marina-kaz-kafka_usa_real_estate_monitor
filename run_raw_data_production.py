from producers.producers import RawDataProducer
from producers.configurations import data_producer_config, data_producer_topic_config

def main() -> None:
    producers = [RawDataProducer(data_producer_config=data_producer_config)
                 for _ in range(3)]
    for producer in producers:
        producer.produce(data_producer_topic_config=data_producer_topic_config)

if __name__ == '__main__':
    main()
