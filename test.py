from kafka_connector import Kafka_Connection
from dm_connector import DeploymentManager
import logger

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
    assert(DM_CON.download_upload_package(test_num))
    assert(DM_CON.deploy_package(test_num))
    assert(DM_CON.create_application(test_num))
    return test_result

KAFKA_CON = None
DM_CON = None

def main():
    global KAFKA_CON
    global DM_CON

    KAFKA_CON = Kafka_Connection("10.0.0.92", 10900, 9092, "rhel-sp1", LOGGER)
    DM_CON = DeploymentManager("10.0.0.166", 5000, 8888, LOGGER)
    start_test1()

if __name__ == "__main__":
    main()
