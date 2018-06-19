import requests

from data_src_ss import ss_producer

class Kafka_Connection(object):

    def __init__(self, kafka_host, km_bind_port, kafka_port, cluster):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.km_bind_port = km_bind_port
        self.cluster_name = cluster

    def create_topic(self, topic, partitions=1, replication=1):
        is_created = False
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"topic": "%s" % topic, "partitions": "%d" % partitions, "replication": "%d" % replication}
        uri = "http://%s:%d/clusters/%s/topics/create" % (self.kafka_host, self.km_bind_port, self.cluster_name)

        res = requests.post(uri, data=data, headers=headers)

        if res.status_code == 200:
            is_created = True

        return is_created

    def run_producer(self, pkg_name, topic, event_count):
        if pkg_name == "spark-batch-wf":
            self._run_ssproducer(topic, event_count)

    def _run_ssproducer(self, topic, event_count):
        ss_producer.produce(self.kafka_host, self.kafka_port, topic, event_count)
