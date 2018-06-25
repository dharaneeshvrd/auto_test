import time
import json
import sys

from kafka_connector import Kafka_Connection
from dm_connector import DeploymentManager
from hdfs_connector import HDFS
from constant_info import APPLICATION_NAME, AVRO_EVENT_COUNT, TOPIC_NAMES, PACKAGE_LIST
import logger
import utils

# constants
EDGE_IP = sys.argv[1]
PEM_FILE = sys.argv[2]
DM_PORT = 5000
PM_PORT = 8888
LOGGER = logger.get_logger()


class AutomatedTests(object):

    def __init__(self, config):
        self.kafka_con = Kafka_Connection(config["kafka_manager"], config["kafka_brokers"], LOGGER)
        self.dm_con = DeploymentManager(EDGE_IP, DM_PORT, PM_PORT, LOGGER)
        self.hdfs_con = HDFS(config["webhdfs_host"], config["webhdfs_port"], "hdfs", LOGGER)
        self._config = config

    def print_results(self, text, status):
        if status ==True:
            LOGGER.info("%s: %s", text, "Success")
        else:
            LOGGER.error("%s: %s", text, "Failure")

    def start_test1(self):
        '''
        Testing spark-batch-wf application
        '''
        LOGGER.info("Testing spark-batch-wf application")

        test_num = 1
        topic = eval("TOPIC_NAMES.TEST%d" % test_num)
        event_count = eval("AVRO_EVENT_COUNT.TEST%d" % test_num)
        pkg_name = eval("PACKAGE_LIST.TEST%d" % test_num)
        application = eval("APPLICATION_NAME.TEST%d" % test_num)
        parquet_file_location = "/user/pnda/parquet/%s" % application

        self.print_results("Creating kafka topic %s" % topic, self.kafka_con.create_topic(test_num))
        self.print_results("Producing %d avro events to %s topic" % (event_count, topic), self.kafka_con.run_producer(test_num))
        self.print_results("Triggering KafkaMR pull job to create datasets", utils.exec_ssh(EDGE_IP, self._config["cluster_root_user"], PEM_FILE, ["sudo /bin/systemctl start gobblin"])[0])

        count = 1
        dataset = "/user/pnda/PNDA_datasets/datasets/source=test-src"
        while(count):
            if self.hdfs_con.file_exists(dataset):
                self.print_results("Dataset is created successfully in %s" % dataset, True)
                break
            else:
                time.sleep(1)
                count += 1
            if count >= 300:
                self.print_results("Dataset is not created successfully in %s" % dataset, False)

        self.print_results("Downloading and uploading %s to package repository" % pkg_name, self.dm_con.download_upload_package(test_num))
        self.print_results("Deploying package %s to package repository" % pkg_name, self.dm_con.deploy_package(test_num))
        self.print_results("Creating application %s" % application, self.dm_con.create_application(test_num))
        self.print_results("Starting application %s" % application, self.dm_con.start_application(test_num))

        time.sleep(30)
        count = 1
        is_parquet_file_available = False
        while(count):
            if self.hdfs_con.file_exists("%s/_SUCCESS" % parquet_file_location):
                is_parquet_file_available = True
                break
            else:
                time.sleep(1)
                count += 1

            if count >= 300:
                break

        self.print_results("Verifying parquet files in %s" % parquet_file_location, is_parquet_file_available)

    def start_test2(self):
        """
        Spark streaming application testing
        """
        LOGGER.info("Testing Spark streaming application")

        test_num = 2
        topic = eval("TOPIC_NAMES.TEST%d" % test_num)
        event_count = eval("AVRO_EVENT_COUNT.TEST%d" % test_num)
        pkg_name = eval("PACKAGE_LIST.TEST%d" % test_num)
        application = eval("APPLICATION_NAME.TEST%d" % test_num)

        self.print_results("Creating kafka topic %s" % topic, self.kafka_con.create_topic(test_num))
        self.print_results("Downloading and uploading %s to package repository" % pkg_name, self.dm_con.download_upload_package(test_num))
        self.print_results("Deploying package %s to package repository" % pkg_name, self.dm_con.deploy_package(test_num))
        self.print_results("Creating application %s" % application, self.dm_con.create_application(test_num))
        self.print_results("Starting application %s" % application, self.dm_con.start_application(test_num))

        time.sleep(30)
        self.print_results("Producing %d avro events to %s topic" % (event_count, topic), self.kafka_con.run_producer(test_num))

        time.sleep(5)
        commands = ["sudo whisper-fetch --json /var/lib/carbon/whisper/application/kpi/%s/message-count.wsp"\
         % eval("APPLICATION_NAME.TEST%d" % test_num)]

        whisper_out = json.loads(utils.exec_ssh(EDGE_IP, self._config["cluster_root_user"], PEM_FILE, commands)[1])

        is_avro_events_processed = False
        if event_count in whisper_out["values"]:
            is_avro_events_processed = True

        self.print_results("Verifying %d avro events processed by %s application on %s topic" % (event_count, application, topic), is_avro_events_processed)

if __name__ == "__main__":
    utils.set_logger(LOGGER)
    config = utils.fill_config(EDGE_IP, DM_PORT)
    tester = AutomatedTests(config)
    tester.start_test1()
    tester.start_test2()
