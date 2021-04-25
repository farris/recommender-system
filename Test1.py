#spark-submit Test.py hdfs:/user/bm106/pub/MSD/cf_train.parquet hdfs:/user/bm106/pub/MSD/cf_validation.parquet hdfs:/user/bm106/pub/MSD/cf_test.parquet


# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer
import sys 

def main(spark, sc,file_path):
    
    sc.setLogLevel("OFF")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    schemaRatings = spark.read.parquet(str(file_path[0]))

###########################################
    # schemaString = "user_str track_id count"

    # fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    # schema = StructType(fields)

    # # Apply the schema to the RDD.
    # schemaRatings = spark.createDataFrame(ratings, schema)
    
    schemaRatings.createOrReplaceTempView("ratings")
###################################################
    indexer_user = StringIndexer(inputCol="user_id", outputCol="user_ID_")
    indexed = indexer_user.fit(schemaRatings).transform(schemaRatings)
    
    indexer_track = StringIndexer(inputCol="track_id", outputCol="trackId")
    indexed = indexer_track.fit(indexed).transform(indexed) 
    
    print("Indexed-----------------------------------------------------------------------------------")
    #print(indexed.show())
    
    
    indexed.createOrReplaceTempView("ratings_idx")
    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT user_id, track_id, count, CAST(user_ID_ AS INT) AS userId , CAST(trackId AS INT) AS trackId FROM ratings_idx")
    print("Results-----------------------------------------------------------------------------------")
    #print(results.show()  )

    results.createOrReplaceTempView("final")
    cleaned = spark.sql("SELECT userId, trackId ,count FROM final")
    print("Cleaned-----------------------------------------------------------------------------------")
    #print(cleaned.show() )
    
    train_rdd = cleaned.rdd.map(tuple)
    print("Train_RDD-----------------------------------------------------------------------------------")
    print(train_rdd.take(10))
    
    # from pyspark.mllib.recommendation import ALS
    # model=ALS.trainImplicit(train_rdd, rank=5, iterations=3, alpha=0.99)

#     print(model)
    
    
    
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc, sys.argv[1:])