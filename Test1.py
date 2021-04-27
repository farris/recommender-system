#spark-submit Test.py hdfs:/user/bm106/pub/MSD/cf_train.parquet hdfs:/user/bm106/pub/MSD/cf_validation.parquet hdfs:/user/bm106/pub/MSD/cf_test.parquet


# And pyspark.sql to get the spark session
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

#%%
def main(spark, sc):
    ################################
    i = 1   ### input file flag ####
    ################################
    file_path = ['hdfs:/user/bm106/pub/MSD/cf_train_new.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_validation.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_test.parquet']
    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    schemaRatings0 = spark.read.parquet(str(file_path[i]))
    schemaRatings = schemaRatings0.sort(col('__index_level_0__'))
    schemaRatings.createOrReplaceTempView("ratings")
    ###################################################
   
                                    
 
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
    
   
    
    results.createOrReplaceTempView("final")
    cleaned = spark.sql("SELECT userId, trackId ,count FROM final")
    

    (training, test) = cleaned.randomSplit([0.8, 0.2])
    #training  = training.rdd
    
###############################################
    als = ALS(rank = 10, maxIter=10, regParam=.001,userCol="userId", itemCol="trackId", ratingCol="count",
                    alpha = .99, implicitPrefs = True,coldStartStrategy="drop")
    model = als.fit(training)
    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="count",
                                predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print('-----------------------------------------------')
    print('-----------------------------------------------')
    print("Root-mean-square error = " + str(rmse))
    print('-----------------------------------------------')
    print('-----------------------------------------------')
    
    
    print('')
    print('')
    print('')



    print('-----------------------------------------------')
    print('Generate top 10 movie recommendations for a specified set of users')
    print('-----------------------------------------------')
    users = cleaned.select(als.getUserCol()).distinct().limit(3)
    userSubsetRecs = model.recommendForUserSubset(users, 10)
    print(userSubsetRecs.show())
    print('-----------------------------------------------')
    print('Generate top 10 user recommendations for a specified set of movies')
    print('-----------------------------------------------') 
    movies = cleaned.select(als.getItemCol()).distinct().limit(3)
    movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    print(movieSubSetRecs.show())
    

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc)