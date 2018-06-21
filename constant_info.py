class PACKAGE_LIST(object):
    TEST1 = "spark-batch-example-app-wf-1.1.2"
    TEST2 = "spark-streaming-example-app-python-1.1.1"

class PACKAGE_LINK(object):
    TEST1 = "https://s3.amazonaws.com/pnda-apps-public/spark-batch-example-app-wf-1.1.2.tar.gz"
    TEST2 = "https://s3.amazonaws.com/pnda-apps-public/spark-streaming-example-app-python-1.1.1.tar.gz"

class TOPIC_NAMES(object):
    TEST1 = "avro.pnda.test21"
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
