import time

from kafka_connector import Kafka_Connection
from dm_connector import DeploymentManager
from hdfs_connector import HDFS
import logger
import utils

# constants
LOGGER = logger.get_logger()

def start_test1():
    '''
    Testing spark-batch-wf application
    '''
    LOGGER.info("Starting test1")
    test_result = False
    test_num = 1
    assert(KAFKA_CON.create_topic(test_num))
    assert(KAFKA_CON.run_producer(test_num))
    assert(utils.exec_ssh("10.0.0.166", "ec2-user", "/opt/pnda/dm_keys/dm.pem", ["sudo /bin/systemctl start gobblin"]))

    count = 1 
    while(count):
        if HDFS_CON.file_exists("/user/pnda/PNDA_datasets/datasets/source=test-src"):
            LOGGER.info("Dataset source=test-src is created")
            break
        else:
            time.sleep(1)
            count += 1
        if count >= 300:
            LOGGER.error("Dataset source=test-src is not created")
            break

    assert(DM_CON.download_upload_package(test_num))
    assert(DM_CON.deploy_package(test_num))
    assert(DM_CON.create_application(test_num))
    assert(DM_CON.start_application(test_num))

    time.sleep(30)
    count = 1
    while(count):
        if HDFS_CON.file_exists("/user/pnda/parquet/test-app1/_SUCCESS"):
            LOGGER.info("Application created the parquet files")
            break
        else:
            time.sleep(1)
            count += 1
        if count >= 300:
            LOGGER.error("Application failed to create parquet files")
            test_result = False
            break
    return test_result

KAFKA_CON = None
DM_CON = None
HDFS_CON = None

def main():
    global KAFKA_CON
    global DM_CON
    global HDFS_CON

    KAFKA_CON = Kafka_Connection("10.0.0.92", 10900, 9092, "rhel-sp1", LOGGER)
    DM_CON = DeploymentManager("10.0.0.166", 5000, 8888, LOGGER)
    HDFS_CON = HDFS("rhel-sp1-hadoop-mgr-1.node.dc1.pnda.local", "14000", "hdfs", LOGGER) 
    utils.set_logger(LOGGER)
    start_test1()

if __name__ == "__main__":
    main()
