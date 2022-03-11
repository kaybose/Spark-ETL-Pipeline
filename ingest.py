#from pyspark.sql.types import *
#from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *



def injest_data(spark, data_dir, file_format, mark):
    #print("Injesting")
    if mark == 'proj':
        print('Injesting proj...')
        cust_schema = StructType([ \
                StructField("pid", StringType(), True), \
                StructField("regionname", StringType(), True), \
                StructField("countryname", StringType(), False), \
                StructField("projectstatus", StringType(), True), \
                StructField("project_name", StringType(), True), \
                StructField("pdo", StringType(), True), \
                StructField("impagency", StringType(), True), \
                StructField("cons_serv_reqd_ind", StringType(), True), \
                StructField("url", StringType(), True), \
                StructField("boardapprovaldate", StringType(), True), \
                StructField("closingdate", StringType(), True), \
                StructField("projectfinancialtype", StringType(), True), \
                StructField("curr_project_cost", IntegerType(), True), \
                StructField("curr_ibrd_commitment", IntegerType(), True), \
                StructField("curr_ida_commitment", IntegerType(), True), \
                StructField("curr_total_commit", IntegerType(), True), \
                StructField("grantamt", IntegerType(), True), \
                StructField("borrower", StringType(), True), \
                StructField("lendinginstr", StringType(), True), \
                StructField("envassesmentcategorycode", StringType(), True), \
                StructField("esrc_ovrl_risk_rate", StringType(), True), \
                StructField("sector1", StringType(), True), \
                StructField("sector2", StringType(), True), \
                StructField("sector3", StringType(), True), \
                StructField("theme1", StringType(), True), \
                StructField("theme2", StringType(), True)])

        df_raw = spark.read.schema(cust_schema). \
            format(file_format). \
            load(data_dir)

        return df_raw

    elif mark == 'code':
        print('Injesting code...')
        df = spark.read. \
            option("header", True). \
            format(file_format). \
            load(data_dir)

        return df