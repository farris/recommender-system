#%% Imports

from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import sys 
# %%
def int_maker(spark,indexed,sc):

    indexed.createOrReplaceTempView("ratings_idx")
    results = spark.sql("""
                            SELECT user_id, track_id, count,__index_level_0__, CAST(user_id_index AS INT) AS userId , \
                                CAST(track_id_index AS INT) AS trackId FROM ratings_idx
            
                            """)

    return results
#%% Function Defintions

def main(spark, sc):
    ###############################################################
    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    ###############################################################
    file_path = ['hdfs:/user/bm106/pub/MSD/cf_train_new.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_validation.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_test.parquet']
    files = ['train','validation','test']
    ###############################################################
    
    ##### Read in data and build uniform index on train
    schemaRatings = spark.read.parquet('hdfs:/user/bm106/pub/MSD/cf_train_new.parquet')
    schemaRatings_train = schemaRatings.sort(col('__index_level_0__'))
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(schemaRatings_train) \
                for column in list(set(schemaRatings_train.columns)-set(['count'])-set(['__index_level_0__'])) ]
    pipeline = Pipeline(stages=indexers)
    
    ##### Fit index to each dataset

    for f,z in zip(file_path,files):
    
        df = spark.read.parquet(f)
        df = df.sort(col('__index_level_0__'))
        indexed = pipeline.fit(df).transform(df)
        final = int_maker(spark,indexed,sc)

        path = 'hdfs:/user/fda239/'+ z +'.parquet'

        final.write.mode("overwrite").parquet(path)


#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc)