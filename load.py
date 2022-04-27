
def load_data(spark, df_transformed, tgt_dir, file_format):
    #print("Loading...")

    df_transformed.write. \
        format(file_format). \
        save(tgt_dir).\
        saveAsTable('finalData')

    #df_transformed.createOrReplaceTempView("finalData")
    #pyspark.sql("")

