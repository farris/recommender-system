from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
import sys 


def main(spark, sc):
    # sample sizes
    train_samples = .005

    print('Reading Files...')
    cf_train = spark.read.parquet('hdfs:/user/zm2114/cf_train.parquet')
    cf_validation = spark.read.parquet('hdfs:/user/zm2114/cf_validation.parquet')
    cf_test = spark.read.parquet('hdfs:/user/zm2114/cf_test.parquet')

    ## Comment out these lines once subsample has been created ##
    # taking x% of training samples
    user_ids = cf_train.select("user_id").distinct()
    training_ids = user_ids.sample(fraction=train_samples, seed=SEED)

    # getting training set with just x% distinct user_ids
    tr_df = training_ids.join(cf_train, ["user_id"], how='left')
    tr_df = tr_df.drop(*['__index_level_0__'])

    # taking val_samp% of validation samples
    vl_df = training_ids.join(cf_validation, ["user_id"], how='left')
    vl_df = vl_df.drop(*['__index_level_0__'])

    # taking val_samp% of validation samples
    ts_df = training_ids.join(cf_test, ["user_id"], how='left')
    ts_df = ts_df.drop(*['__index_level_0__'])

    tr_df.coalesce(1).write.option("header", "true").parquet('train_subsample')
    vl_df.coalesce(1).write.option("header", "true").parquet('val_subsample')  
    ts_df.coalesce(1).write.option("header", "true").parquet('test_subsample')
    # ## ## ##
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Subsample').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)
