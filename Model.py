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

    train = format(spark.read.parquet(file_path[0]))   
#     val = format(spark.read.parquet(file_path[1]))
    test = format(spark.read.parquet(file_path[2]))
    print('read in data')  
    print('----------------')      
    #Training#####################################################
    als = ALS(rank = 2, maxIter=3, regParam=.01, alpha = 20, 
              userCol="userId", itemCol="trackId", ratingCol="count", implicitPrefs = True)

    model = als.fit(train) ##train
    print('training complete')  
    print('----------------')   
    
    # -------------------- Running full model. - This ran successfully -------------------------
    # ------------------------------ 10 Recs for each user -------------------------------------
    users = test.select(als.getUserCol()).distinct().limit(10)
    userSubsetRecs = model.recommendForUserSubset(users, 10)
    userSubsetRecs = userSubsetRecs.select("userId","recommendations.trackId")
    print("Showing userSubsetRecs")
    userSubsetRecs.show()
    
    test = test.groupBy("userId").agg(F.collect_list("trackId").alias("trackId_preds"))
    test.show(truncate=False)
    
    
    k = userSubsetRecs.join(test,"userId")
    k = k.select('trackId_preds',"trackId").rdd
    
    print("-------------------- MAP ------------------------")
    metrics = RankingMetrics(k)
    
    print(metrics.meanAveragePrecision)
    
#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Model').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)