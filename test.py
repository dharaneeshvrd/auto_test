import sys
import logging

from kafka_connector import Kafka_Connection
import pdb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def start_test1():
    '''
    Testing spark-batch-wf application
    '''
    test_result = False
    topic = "new_topic1"
    avro_event_count = 10

    assert(KAFKA_CON.create_topic(topic))
    logger.info("Created new kafak topic %s", topic)

    KAFKA_CON.run_producer("spark-batch-wf", topic, 10)
    logger.info("Produced %d events to %s topic", avro_event_count, topic)

    return test_result

KAFKA_CON = None

def main():
    global KAFKA_CON

    KAFKA_CON = Kafka_Connection("10.0.0.92", 10900, 9092, "rhel-sp1")
    start_test1()

if __name__ == "__main__":
    main()
