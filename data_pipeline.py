from util import get_spark_session
import os

from ingest import injest_data
from transform import transform_data
from load import load_data


def run_pipeline(spark, src_dir_proj, src_dir_code, file_format):

        print("Running Pipeline")

        df_projects = injest_data(spark, src_dir_proj, file_format, mark='proj')
        df_code = injest_data(spark, src_dir_code, file_format, mark = 'code')

        df_transformed = transform_data(spark, df_projects, df_code)

        #load_data(spark, df_transformed, tgt_dir, tgt_file_format)




if __name__ == '__main__':
    env = os.environ.get('ENVIRON')
    src_dir_proj = os.environ.get('SRC_DIR_PROJ')
    src_dir_code = os.environ.get('SRC_DIR_CODE')
    #src_file_pattern = f'{os.environ.get("SRC_FILE_PATTERN")}'
    src_file_format = os.environ.get('SRC_FILE_FORMAT')

    tgt_dir = os.environ.get('TGT_DIR')
    tgt_file_format = os.environ.get('TGT_FILE_FORMAT')
    print('in main')
    spark = get_spark_session(env,'World Bank ETL')
    print('after sc,going to running pipe')
    run_pipeline(spark, src_dir_proj, src_dir_code, src_file_format)
    #demo comment
