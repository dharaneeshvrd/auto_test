import requests

from data_src_sb import sb_producer
from data_src_ss import ss_producer
from constant_info import PACKAGE_LIST, TOPIC_NAMES, AVRO_EVENT_COUNT

# constants
LOGGER = None

class Kafka_Connection(object):

    def __init__(self, kafka_manager, kafka_broker, logger):
        self.kafka_manager = kafka_manager
        self.kafka_broker = kafka_broker

        global LOGGER
        LOGGER = logger

    def create_topic(self, test_num, partitions=1, replication=1):
        is_created = False
        topic = eval("TOPIC_NAMES.TEST%d" % test_num)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"topic": "%s" % topic, "partitions": "%d" % partitions, "replication": "%d" % replication}
        uri = "%s/topics/create" % self.kafka_manager

        res = requests.post(uri, data=data, headers=headers)
        if res.status_code == 200:
            LOGGER.info("Created new kafka topic %s", topic)
            is_created = True

        return is_created

    def run_producer(self, test_num):
        producer_result = False
        pkg_name = eval("PACKAGE_LIST.TEST%d" % test_num)
        topic = eval("TOPIC_NAMES.TEST%d" % test_num)
        event_count = eval("AVRO_EVENT_COUNT.TEST%d" % test_num)
        if test_num == 1:
            producer_result = self._run_producer(topic, event_count, "sb")
        elif test_num == 2:
            producer_result = self._run_producer(topic, event_count, "ss")

        return producer_result

    def _run_producer(self, topic, event_count, producer_type):
        is_successfully_produced = False
        produced_count = eval("%s_producer.produce(self.kafka_broker, topic, event_count)" % producer_type)
        if produced_count == event_count:
            LOGGER.info("%d events produced to %s", produced_count, topic) 
            is_successfully_produced = True
        else:
            LOGGER.debug("ssproducer failed to produce events to topic %s", topic)
        return is_successfully_produced
