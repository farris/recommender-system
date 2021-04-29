from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
import sys 


def main(spark, sc):
    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
    schemaRatings = spark.read.parquet('hdfs:/user/bm106/pub/MSD/cf_train_new.parquet').select('user_id','track_id','count')
    schemaRatings = schemaRatings.repartition(1000)
#     print(schemaRatings.rdd.getNumPartitions())
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").setHandleInvalid("skip").fit(schemaRatings) \
               for column in list(set(schemaRatings.columns)-set(['count'])) ]
        
    
    pipeline = Pipeline(stages=indexers)
    indexed = pipeline.fit(schemaRatings)
    path = 'hdfs:/user/zm2114/hash'
    indexed.write().overwrite().save(path) 

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Build_Hash').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)
