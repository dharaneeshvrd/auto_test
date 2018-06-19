import logging
import requests

class Kafka_Connection(object):

    def __init__(self, km_host, km_bind_port, cluster):
        self.km_host = km_host
        self.km_bind_port = km_bind_port
        self.cluster_name = cluster

    def create_topic(self, topic, partitions=1, replication=1):
        is_created = False
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"topic": "%s" % topic, "partitions": "%d" % partitions, "replication": "%d" % replication}
        uri = "http://%s:%d/clusters/%s/topics/create" % (self.km_host, self.km_bind_port, self.cluster_name)

        res = requests.post(uri, data=data, headers=headers)
        if res.status_code == 200:
            logging.info("Created a new kafka topic: %s", topic)
            is_created = True
        else:
            logging.debug("failed to create topic %s", topic)

        return is_created
