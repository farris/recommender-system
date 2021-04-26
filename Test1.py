#spark-submit Test.py hdfs:/user/bm106/pub/MSD/cf_train.parquet hdfs:/user/bm106/pub/MSD/cf_validation.parquet hdfs:/user/bm106/pub/MSD/cf_test.parquet


# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
import sys 



from pyspark.ml import Pipeline

#%%
def main(spark, sc):
    ################################
    i = 0   ### input file flag ####
    ################################
    file_path = ['hdfs:/user/bm106/pub/MSD/cf_train_new.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_validation.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_test.parquet']
    sc.setLogLevel("OFF")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    schemaRatings0 = spark.read.parquet(str(file_path[i]))
    schemaRatings = schemaRatings0.sort(col('__index_level_0__'))

    ###################################################
    schemaRatings.createOrReplaceTempView("ratings")
                                    
 
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(schemaRatings) \
        for column in list(set(schemaRatings.columns)-set(['count'])-set(['count'])-set(['__index_level_0__'])) ]


    pipeline = Pipeline(stages=indexers)
    indexed = pipeline.fit(schemaRatings).transform(schemaRatings)


#%%
    # print("Indexed-----------------------------------------------------------------------------------")
    # print(indexed.show())
    
    # ###################################################
    indexed.createOrReplaceTempView("ratings_idx")

    results = spark.sql("""
                            SELECT user_id, track_id, count,__index_level_0__, CAST(user_id_index AS INT) AS userId , \
                                CAST(track_id_index AS INT) AS trackId FROM ratings_idx
                            
                            
                        
                            
                            """)
    
    # print("Results-----------------------------------------------------------------------------------")
    #print(results.show()  )
    
    results.createOrReplaceTempView("final")
    cleaned = spark.sql("SELECT userId, trackId ,count FROM final")
    #cleaned = results.rdd
    # print("Cleaned-----------------------------------------------------------------------------------")
    # print(cleaned.show() )
    
    #train_rdd = cleaned.rdd
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
    main(spark, sc)