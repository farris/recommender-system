#spark-submit Test.py hdfs:/user/bm106/pub/MSD/cf_train.parquet hdfs:/user/bm106/pub/MSD/cf_validation.parquet hdfs:/user/bm106/pub/MSD/cf_test.parquet


# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
import sys 



from pyspark.ml import Pipeline


def main(spark, sc,file_path):

    
    sc.setLogLevel("OFF")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    schemaRatings0 = spark.read.parquet(str(file_path[0]))
    schemaRatings = schemaRatings0.sort(col('__index_level_0__'))
    print("Original Size-----------------------------------------------------------------------------------")
    #print(initial1.count())
    
    print("Sample Size-----------------------------------------------------------------------------------")
    #schemaRatings = initial1.sample(0.001, 123)

    ###################################################
    schemaRatings.createOrReplaceTempView("ratings")
    print('1---------')                                         #user_id_index
    # indexer_user = StringIndexer(inputCol="user_id", outputCol="user_ID_")
    # indexed = indexer_user.fit(schemaRatings).transform(schemaRatings)
    
    # print("2---------")
                                                                    #track_id_index
    # indexer_track = StringIndexer(inputCol="track_id", outputCol="trackId")
    
    # print("3---------")
    
    # indexed = indexed.sort(col("track_id"))

    # print("4---------")
    # indexed = indexer_track.fit(indexed).transform(indexed) 
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(schemaRatings) \
        for column in list(set(schemaRatings.columns)-set(['count'])-set(['count'])-set(['__index_level_0__'])) ]


    pipeline = Pipeline(stages=indexers)
    indexed = pipeline.fit(schemaRatings).transform(schemaRatings)
    


#%%
    # print("Indexed-----------------------------------------------------------------------------------")
    # print(indexed.show())
    
    # ###################################################
    indexed.createOrReplaceTempView("ratings_idx")

    results = spark.sql("SELECT user_id, track_id, count, CAST(user_id_index AS INT) AS userId , CAST(track_id_index AS INT) AS trackId FROM ratings_idx")
    # print("Results-----------------------------------------------------------------------------------")
    # print(results.show()  )

    # results.createOrReplaceTempView("final")
    # cleaned = spark.sql("SELECT userId, trackId ,count FROM final")

    # print("Cleaned-----------------------------------------------------------------------------------")
    # print(cleaned.show() )
    
    # train_rdd = cleaned.rdd.map(tuple)
    # print("Train_RDD-----------------------------------------------------------------------------------")
    
    # print(train_rdd.take(10))
    
    # from pyspark.mllib.recommendation import ALS
    # model=ALS.trainImplicit(train_rdd, rank=5, iterations=3, alpha=0.99)

    # print(model)
    
    
    
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc, sys.argv[1:])