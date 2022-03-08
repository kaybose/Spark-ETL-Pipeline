from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, col, lit
from pyspark.sql.types import *

class Injest:
    def __init__(self,spark):
        self.spark = spark


    def injest_data(self,spark):
        print("Injesting")
        cust_schema = StructType([ \
            StructField("pid", StringType(), True), \
            StructField("regionname", StringType(), True), \
            StructField("countryname", StringType(), False), \
            StructField("boardapprovaldate", TimestampType(), True), \
            StructField("closingdate", TimestampType(), True), \
            StructField("curr_project_cost", IntegerType(), True), \
            StructField("curr_ibrd_commitment", IntegerType(), True), \
            StructField("curr_ida_commitment", IntegerType(), True), \
            StructField("curr_total_commit", IntegerType(), True), \
            StructField("grantamt", IntegerType(), True)])
        df = self.spark.read.schema(cust_schema).csv('/FileStore/tables/wb_projects_demo.csv')
        return df