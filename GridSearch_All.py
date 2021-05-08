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
    
    # -------------------- Running full model. - This ran successfully -------------------------
    # ------------------------------ 10 Recs for each user -------------------------------------
    #            alpha                      regParam                                                    maxIter                   rank
    params = [ [10],                          [1]    ,                           [10]     ,           [150]        ] 
    params = list(itertools.product(*params))
    #params = params[0:2]
    precision = []

    val1 = val.groupBy("userId").agg(F.collect_list("trackId").alias("trackId_preds"))

    for i in params:
        
        als = ALS(rank = i[3], maxIter=i[2],regParam=i[1],userCol="userId", itemCol="trackId", ratingCol="count",
                    alpha = i[0], implicitPrefs = True,coldStartStrategy="drop")

        model = als.fit(train) ##train   
        ##############################################################

        ##############################################################
        users = val.select(als.getUserCol()).distinct()
        userSubsetRecs = model.recommendForUserSubset(users, 500)
        userSubsetRecs = userSubsetRecs.select("userId","recommendations.trackId")
        
    
        k = userSubsetRecs.join(val1,"userId")
        k = k.select('trackId_preds',"trackId").rdd
        
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
    
    # print('BEST----------------------------------------------------')
    # idx_max = np.argmax(precision)
    # best_params = params[idx_max]
    # print("alpha= " + str(best_params[0]))
    # print("regParam= " + str(best_params[1]))
    # print("maxIter= " + str(best_params[2]))
    # print("rank= " + str(best_params[3]))
    # print("MAP= " + np.max(precision))
    # print('-----------------------------------------------------')

    
#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Model').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)