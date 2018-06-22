import time
import json
import sys

from kafka_connector import Kafka_Connection
from dm_connector import DeploymentManager
from hdfs_connector import HDFS
from constant_info import APPLICATION_NAME, AVRO_EVENT_COUNT
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

    def start_test1(self):
        '''
        Testing spark-batch-wf application
        '''
        LOGGER.info("Starting test1")
        test_result = False
        test_num = 1
        assert(self.kafka_con.create_topic(test_num))
        assert(self.kafka_con.run_producer(test_num))
        assert(utils.exec_ssh(EDGE_IP, self._config["cluster_root_user"], PEM_FILE, ["sudo /bin/systemctl start gobblin"])[0])

        count = 1
        while(count):
            if self.hdfs_con.file_exists("/user/pnda/PNDA_datasets/datasets/source=test-src"):
                LOGGER.info("Dataset source=test-src is created")
                break
            else:
                time.sleep(1)
                count += 1
            if count >= 300:
                LOGGER.error("Dataset source=test-src is not created")
                return test_result

        assert(self.dm_con.download_upload_package(test_num))
        assert(self.dm_con.deploy_package(test_num))
        assert(self.dm_con.create_application(test_num))
        assert(self.dm_con.start_application(test_num))

        time.sleep(30)
        count = 1
        while(count):
            if self.hdfs_con.file_exists("/user/pnda/parquet/%s/_SUCCESS" % eval("APPLICATION_NAME.TEST%d" % test_num)):
                LOGGER.info("Application created the parquet files")
                test_result = True
                break
            else:
                time.sleep(1)
                count += 1
            if count >= 300:
                LOGGER.error("Application failed to create parquet files")
                break

        return test_result

    def start_test2(self):
        """
        Spark streaming application testing
        """
        LOGGER.info("Starting test2")
        test_result = False
        test_num = 2
        assert(self.kafka_con.create_topic(test_num))
        assert(self.dm_con.download_upload_package(test_num))
        assert(self.dm_con.deploy_package(test_num))
        assert(self.dm_con.create_application(test_num))
        assert(self.dm_con.start_application(test_num))
        time.sleep(30)
        assert(self.kafka_con.run_producer(test_num))
        time.sleep(2)
        commands = ["sudo whisper-fetch --json /var/lib/carbon/whisper/application/kpi/%s/message-count.wsp"\
         % eval("APPLICATION_NAME.TEST%d" % test_num)]
        whisper_out = json.loads(utils.exec_ssh(EDGE_IP, self._config["cluster_root_user"], PEM_FILE, commands)[1])
        if eval("AVRO_EVENT_COUNT.TEST%d" % test_num) in whisper_out["values"]:
            LOGGER.info("Avro events produced are successfully processed")
            test_result = True
        else:
            LOGGER.error("Avro events produced are not processed by the application")
        return test_result

if __name__ == "__main__":
    utils.set_logger(LOGGER)
    config = utils.fill_config(EDGE_IP, DM_PORT)
    tester = AutomatedTests(config)
    assert(tester.start_test1())
    LOGGER.info("**************TEST1: PASS**************")
    assert(tester.start_test2())
    LOGGER.info("**************TEST2: PASS**************")
