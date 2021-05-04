# %% Imports
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as F 
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

#%%
def format(df):
    df = df.select('userId',"trackId","count") 
    return df

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
    train = format(spark.read.parquet(file_path[0]))   
    val = format(spark.read.parquet(file_path[1]))
    test = format(spark.read.parquet(file_path[2]))
    print('read in data')  
    print('----------------')      
    #Training#####################################################
#     als = ALS(rank = 2, maxIter=1, regParam=.001,userCol="userId", itemCol="trackId", ratingCol="count",
#                 alpha = .99, implicitPrefs = True,coldStartStrategy="drop")

#     model = als.fit(train) ##train
#     print('training complete')  
#     print('----------------')   
    ##############################################################

    #error########################################################
#     predictions = model.transform(test)  ##test & make predictions
    

#     user_list = [row['userId'] for row in test.select(als.getUserCol()).distinct().collect()]  ##get list of users
    
#     userSubsetRecs = model.recommendForUserSubset(test.where(test.userId == user_list[0]), 10) ## make reccs for a given user
#                                                                             ##0 - i

#     userSubsetRecs.printSchema()
#     userSubsetRecs = userSubsetRecs.select("userId","recommendations.trackId") ##unpack nested recc structure
#     print("Showing userSubsetRecs")
#     userSubsetRecs.show()
    
#     print('-----------------------------------------------')
#     ground_truth = test.where(test.userId == user_list[0]).orderBy('count', ascending=False)
#     ground_truth =  ground_truth.groupBy("userId").agg(F.collect_list("trackId"))
#     print("Showing ground truth")
#     ground_truth.show()
    
    
#     print('-----------------------------------------------')
#     k = userSubsetRecs.join(ground_truth,"userId")
#     print("Joined table of preds and labels")
#     k.show()
    
#     print("RDD for joined table")
#     k = k.select('collect_list(trackId)',"trackId").rdd
#     print(k.take(1))
    
#     print("-------------------- MAP ------------------------")
#     metrics = RankingMetrics(k)
    
#     print(metrics.meanAveragePrecision)


    # Ordering by multiple columns. - This works
#     test.orderBy(F.desc('userId'), F.desc('count')).show(40)

#-------------------------------------------------------------------------------------    
    
    # -------------------- Running full model. - This ran successfully -------------------------
    # ------------------------------ 10 Recs for each user -------------------------------------
#     users = test.select(als.getUserCol()).distinct()
#     userSubsetRecs = model.recommendForUserSubset(users, 10)
#     userSubsetRecs = userSubsetRecs.select("userId","recommendations.trackId")
#     print("Showing userSubsetRecs")
#     userSubsetRecs.show()
    
#     test = test.groupBy("userId").agg(F.collect_list("trackId").alias("trackId_preds"))
#     test.show()
    
    
#     k = userSubsetRecs.join(test,"userId")
#     k = k.select('trackId_preds',"trackId").rdd
    
#     print("-------------------- MAP ------------------------")
#     metrics = RankingMetrics(k)
    
#     print(metrics.meanAveragePrecision)
  

    # Grid searching
    
    als = ALS(userCol="userId", itemCol="trackId", ratingCol="count", implicitPrefs = True, coldStartStrategy="drop"
              rank = 2)
    
    paramGrid = ParamGridBuilder() \
        .addGrid(als.alpha, [0.1, 0.99]) \
        .addGrid(als.regParam, [0.001, 0.01]) \
        .addGrid(als.maxIter, [1, 2]) \
        .build()

    cv = CrossValidator(estimator=als,
                          estimatorParamMaps=paramGrid,
                          evaluator=RankingMetrics(),
                          numFolds=2)  # use 3+ folds in practice

    # Run cross-validation, and choose the best set of parameters.
    cvModel = cv.fit(training)
    print('training complete')
    
#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Model').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)