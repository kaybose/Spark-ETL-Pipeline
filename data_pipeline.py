import pyspark
from pyspark.sql import SparkSession

import extract
import transform
import load



class Pipeline:

    def run_pipeline(self):

        print("Running Pipeline")
        extract_process = extract.Extract(self.spark)
        extract_process.extract_data()

        transform_process = transform.Transform(self.spark)
        transform_process.transform_data(df)

        load_process = load.Load(self.spark)
        load_process.load_data()

    def create_spark_session(self):
        self.spark = SparkSession. \
                builder. \
                appName('World Bank ETL'). \
                master('local[*]'). \
                getOrCreate()

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
    #demo comment
