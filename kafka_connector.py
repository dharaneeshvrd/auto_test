import requests

from data_src_ss import ss_producer
from constant_info import PACKAGE_LIST, TOPIC_NAMES, AVRO_EVENT_COUNT

# constants
LOGGER = None

class Kafka_Connection(object):

    def __init__(self, kafka_host, km_bind_port, kafka_port, cluster, logger):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.km_bind_port = km_bind_port
        self.cluster_name = cluster

        global LOGGER
        LOGGER = logger

    def create_topic(self, test_num, partitions=1, replication=1):
        is_created = False
        topic = eval("TOPIC_NAMES.TEST%d" % test_num)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"topic": "%s" % topic, "partitions": "%d" % partitions, "replication": "%d" % replication}
        uri = "http://%s:%d/clusters/%s/topics/create" % (self.kafka_host, self.km_bind_port, self.cluster_name)

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
            producer_result = self._run_ssproducer(topic, event_count)

        return producer_result

    def _run_ssproducer(self, topic, event_count):
        is_successfully_produced = False
        produced_count = ss_producer.produce(self.kafka_host, self.kafka_port, topic, event_count)
        if produced_count == event_count:
            LOGGER.info("%d events produced to %s", produced_count, topic) 
            is_successfully_produced = True
        else:
            LOGGER.debug("ssproducer failed to produce events to topic %s", topic)
        return is_successfully_produced
