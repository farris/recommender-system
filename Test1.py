
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

#%% Function Defintions

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

#%%  Initializing Indexes
 
    # indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(schemaRatings) \
    #     for column in list(set(schemaRatings.columns)-set(['count'])-set(['__index_level_0__'])) ]

    # pipeline = Pipeline(stages=indexers)
    # indexed = pipeline.fit(schemaRatings).transform(schemaRatings)
    path = 'hdfs:/user/fda239/hash'
    pipelineModel = Pipeline.load(path)
    indexed = pipelineModel.transform(schemaRatings)


#%% Converting Indexes to Int type & selecting necessary columns

    indexed.createOrReplaceTempView("ratings_idx")
    results = spark.sql("""
                            SELECT user_id, track_id, count,__index_level_0__, CAST(user_id_index AS INT) AS userId , \
                                CAST(track_id_index AS INT) AS trackId FROM ratings_idx
                            
                                           
                            """)
    results.show()
    
    # results.createOrReplaceTempView("final")
    # cleaned = spark.sql("SELECT userId, trackId ,count FROM final")
    
#%% Training the model

    # #Test/train split & Fit#######################################
    # (training, test) = cleaned.randomSplit([0.8, 0.2])
    # als = ALS(rank = 10, maxIter=10, regParam=.001,userCol="userId", itemCol="trackId", ratingCol="count",
    #                 alpha = .99, implicitPrefs = True,coldStartStrategy="drop")
    # model = als.fit(training)
    # ##############################################################

    # #error########################################################
    # predictions = model.transform(test)
    # evaluator = RegressionEvaluator(metricName="rmse", labelCol="count",
    #                             predictionCol="prediction")
    # rmse = evaluator.evaluate(predictions)
    # ##############################################################

    # print('-----------------------------------------------')
    # print('-----------------------------------------------')
    # print("Root-mean-square error = " + str(rmse))
    # print('-----------------------------------------------')
    # print('-----------------------------------------------')
    
    # print('')
    # print('')
    # print('')

    # print('-----------------------------------------------')
    # print('Generate top 10 movie recommendations for a specified set of users')
    # print('-----------------------------------------------')
    # users = cleaned.select(als.getUserCol()).distinct().limit(3)
    # userSubsetRecs = model.recommendForUserSubset(users, 10)
    # userSubsetRecs.show(truncate=False)
    # print('-----------------------------------------------')
    # print('Generate top 10 user recommendations for a specified set of movies')
    # print('-----------------------------------------------') 
    # movies = cleaned.select(als.getItemCol()).distinct().limit(3)
    # movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    # movieSubSetRecs.show(truncate=False)



    
#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc)