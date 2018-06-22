class PACKAGE_LIST(object):
    TEST1 = "spark-batch-example-app-wf-1.1.2"
    TEST2 = "spark-streaming-example-app-python-1.1.1"

class PACKAGE_LINK(object):
    TEST1 = "https://s3.amazonaws.com/pnda-apps-public/spark-batch-example-app-wf-1.1.2.tar.gz"
    TEST2 = "https://s3.amazonaws.com/pnda-apps-public/spark-streaming-example-app-python-1.1.1.tar.gz"

class TOPIC_NAMES(object):
    TEST1 = "avro.pnda.test-topic1"
    TEST2 = "test-topic2"

class AVRO_EVENT_COUNT(object):
    TEST1 = 10
    TEST2 = 10

class APPLICATION_NAME(object):
    TEST1 = "test-app1"
    TEST2 = "test-app2"

class APPLICATION_PAYLOAD(object):
    spark_batch_example_app_wf = {
        "package": "spark-batch-example-app-wf-1.1.2",
        "oozie": {
        "example":{
            "freq_in_mins":"180",
            "start":"${deployment_start}",
            "end":"${deployment_end}",
            "input_data": "/user/pnda/PNDA_datasets/datasets/source=test-src/year=*"
            }
        }
    }
    spark_streaming_example_app_python = {
        "package": "spark-streaming-example-app-python-1.1.1",
        "sparkStreaming": {
        "example":{
            "main_py": "job.py",
            "py_files": "dataplatform-raw.avsc,avro-1.8.1-py2.7.egg",
            "log_level":"INFO",
            "batch_size_seconds": "2",
            "processing_parallelism": "1",
            "checkpoint_path":"",
            "input_topic": "test-topic2",
            "consume_from_beginning": "false",
            "spark_submit_args": "--conf spark.yarn.executor.memoryOverhead=500 --jars hdfs:///pnda/deployment/app_packages/kafka-clients-0.8.2.2.jar,hdfs:///pnda/deployment/app_packages/spark-streaming-kafka_2.10-1.6.0.jar,hdfs:///pnda/deployment/app_packages/kafka_2.10-0.8.2.2.jar,hdfs:///pnda/deployment/app_packages/metrics-core-2.2.0.jar"
            }
        }
    }
