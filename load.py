
def load_data(spark, df_transformed, tgt_dir, file_format):
    print("Loading...")

    spark.write. \
        format(file_format). \
        save(tgt_dir)


