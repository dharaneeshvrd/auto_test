import logging

from kafka_connector import Kafka_Connection

def start_test1():
    if KAFKA_CON.create_topic("new_topic2"):
        pass

KAFKA_CON = None

def main():
    global KAFKA_CON

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
    KAFKA_CON = Kafka_Connection("10.0.0.92", 10900, "rhel-sp1")
    start_test1()

if __name__ == "__main__":
    main()
