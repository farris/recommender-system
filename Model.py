# %% Imports
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator 
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# def format(df):
#     df = df.select('userId',"trackId","count") 
#     return df.rdd

#%% Main

def main(spark, sc):
      
    #spark configs
    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    
  
    #read in file
    file_path = ['hdfs:/user/zm2114/cf_train_new.parquet',
                 'hdfs:/user/zm2114/cf_validation.parquet',
                 'hdfs:/user/zm2114/cf_test.parquet']

    # train = format(spark.read.parquet(file_path[0]))   
    # val = format(spark.read.parquet(file_path[1]))
    # test = format(spark.read.parquet(file_path[2]))
    
    
    # model = ALS.trainImplicit(train, rank = 3, iterations=2, \
    #                         lambda_=0.01, blocks=-1, alpha=0.01,
    #                             nonnegative=False, seed=None)

    # testdata = val.map(lambda p: (p[0], p[1]))
    # print(testdata.take(5))
    # print('-----------------------------------------------')
    # predictions = model.recommendProducts(1003178, 5)
    # predictions = predictions.rdd
    # print('-----------------------------------------------')

    # testData = ratings.map(lambda p: (p.user, p.product))
    # predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating)) 
    # 
    # 
    # 
    # %%                       
    # 
    train = spark.read.parquet(file_path[0])
    val = spark.read.parquet(file_path[1])
    test = spark.read.parquet(file_path[2])
    print('read in data')  
    print('----------------')      
    #Training#####################################################
    als = ALS(rank = 2, maxIter=1, regParam=.001,userCol="userId", itemCol="trackId", ratingCol="count",
                alpha = .99, implicitPrefs = True,coldStartStrategy="drop")

    model = als.fit(train) ##train
    print('training complete')  
    print('----------------')   
    ##############################################################

    #error########################################################
    predictions = model.transform(test)  ##test
    
    #users = test.select(als.getUserCol()).distinct().limit(2)
   # users_df = test.select(als.getUserCol()).distinct()
    user_list = [row['userId'] for row in test.select(als.getUserCol()).distinct().collect()]
    
    userSubsetRecs = model.recommendForUserSubset(test.where(test.userId == user_list[0]), 10)
    
    
    print(userSubsetRecs.show(truncate = False))  
    userSubsetRecs = userSubsetRecs.rdd

    # evaluator = pyspark.ml.evaluation.RankingEvaluator(metricName="meanAveragePrecision", labelCol="count",
    #                             predictionCol="prediction")
    # MAP = evaluator.evaluate(predictions)
    ##############################################################

    # print('-----------------------------------------------')
    # print('-----------------------------------------------')
    # print("MAP = " + str(MAP))
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