from pyspark.sql import SparkSession


def get_spark_session(env, app_name):
    if env == 'DEV':
        print('DEV mode')
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            getOrCreate()
        return spark
