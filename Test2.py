# %% Imports
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import sys 
# %% Convert Mapped Index Columns to Int Type
def cleaner(spark,indexed,sc):
    
    indexed.createOrReplaceTempView("ratings_idx")
    results = spark.sql("""
                            SELECT user_id, track_id, count,__index_level_0__, CAST(user_id_index AS INT) AS userId , \
                                CAST(track_id_index AS INT) AS trackId FROM ratings_idx
                                        
                            """)
    return results
#%% Main

def main(spark, sc):
    
    #Train set,Test Set##################################################
    i = [1,2]   #### input file flag #### 0 = Train | 1 = Val | 2 = Test
    #Configs#############################################################
    file_path = ['hdfs:/user/bm106/pub/MSD/cf_train_new.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_validation.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_test.parquet']
    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    path = 'hdfs:/user/fda239/hash'
    pipelineModel = PipelineModel.load(path)


    out = []
    for j in i:
        schemaRatings = spark.read.parquet(file_path[j])                ## read in file
        schemaRatings = schemaRatings.sort(col('__index_level_0__'))    ## sort on index
        schemaRatings = pipelineModel.transform(schemaRatings)          ## add float mapped columns for user and track
        out.append(       cleaner(spark,schemaRatings,sc)         )     ## convert float mapped columns to integer
                                                                        ## call cleaner function
    
    training,test = out   ##unpack list
    ##############################################################
    training.createOrReplaceTempView("training")
    results = spark.sql("""
                            SELECT user_id, track_id FROM training
                            WHERE userId = 700105
                     
                            """)

    results.show()
    print('-----------------------------------------------')
    test.createOrReplaceTempView("test")
    results1 = spark.sql("""
                            SELECT user_id, track_id FROM test
                            WHERE userId = 700105
                     
                            """)
    results1.show()

    # #Training#####################################################
    # als = ALS(rank = 10, maxIter=7, regParam=.001,userCol="userId", itemCol="trackId", ratingCol="count",
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
    # users = test.select(als.getUserCol()).distinct().limit(3)
    # userSubsetRecs = model.recommendForUserSubset(users, 10)
    # userSubsetRecs.show(truncate=False)
    # print('-----------------------------------------------')
    # print('Generate top 10 user recommendations for a specified set of movies')
    # print('-----------------------------------------------') 
    # movies = test.select(als.getItemCol()).distinct().limit(3)
    # movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    # movieSubSetRecs.show(truncate=False)
    
#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)