# %% Imports
from pyspark.sql import SparkSession
from pyspark import SparkContext,  SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as F 
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import itertools
import numpy as np
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
    val = format(spark.read.parquet(file_path[1]))
    test = format(spark.read.parquet(file_path[2]))
    

#-------------------------------------------------------------------------------------    
    
    # --------------------         Grid Search Initialization        -------------------------
    # ------------------------------ 500 Recs for each user -------------------------------------
    params = [ [.001,.01,1,10,100],                         #alpha
            [.01,.1,1,10,50]    ,                           #regParam                            
            [3]     ,                                       #maxIter 
            [50,100]        ]                               #rank

    params = list(itertools.product(*params))
    val1 = val.groupBy("userId").agg(F.collect_list("trackId").alias("trackId_preds"))

    precision = []
    for i,z in zip(params,range(len(params))):
        
        als = ALS(rank = i[3], maxIter=i[2],regParam=i[1],userCol="userId", itemCol="trackId", ratingCol="count",
                    alpha = i[0], implicitPrefs = True)
        model = als.fit(train) ##train   

        users = val.select(als.getUserCol()).distinct()

        userSubsetRecs = model.recommendForUserSubset(users, 500)
        userSubsetRecs = userSubsetRecs.select("userId","recommendations.trackId")


        k = userSubsetRecs.join(val1,"userId")
        k = k.rdd.map(lambda row: (row[1], row[2]))

        metrics = RankingMetrics(k)
        precision.append(metrics.meanAveragePrecision)

    for i in range(len(params)):
        print(params[i])
        print("alpha= " + str(params[i][0]))
        print("regParam= " + str(params[i][1]))
        print("maxIter= " + str(params[i][2]))
        print("rank= " + str(params[i][3]))
        print("MAP= " + str(precision[i]))
        print('-----------------------------------------------------')

#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Model').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)