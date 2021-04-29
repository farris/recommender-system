from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel
import sys 

def cleaner(spark,sc,indexed):
    
    indexed.createOrReplaceTempView("ratings_idx")
    results = spark.sql("""
                            SELECT user_id, track_id, count, CAST(user_id_index AS INT) AS userId , \
                                CAST(track_id_index AS INT) AS trackId FROM ratings_idx             
                            """)
    return results

def main(spark, sc, file_type):

    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    #Read in the written hasher
    path = 'hdfs:/user/zm2114/hash' 
    pipelineModel = PipelineModel.load(path)
    
    df = spark.read.parquet('hdfs:/user/bm106/pub/MSD/cf_'+ str(file_type) +'.parquet').select('user_id','track_id','count')
    df = df.repartition(1000)
    df = pipelineModel.transform(df) 
    final = cleaner(spark,sc,df)

    path = 'hdfs:/user/zm2114/cf_'+ str(file_type) +'.parquet'

    final.write.mode("overwrite").parquet(path)

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Parquet_Build').getOrCreate()
    sc = spark.sparkContext
    
    file_type = sys.argv[1]
    
    main(spark, sc, file_type)