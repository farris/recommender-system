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


def apk(actual, predicted, k=10):
    """
    Computes the average precision at k.
    This function computes the average prescision at k between two lists of
    items.
    Parameters
    ----------
    actual : list
             A list of elements that are to be predicted (order doesn't matter)
    predicted : list
                A list of predicted elements (order does matter)
    k : int, optional
        The maximum number of predicted elements
    Returns
    -------
    score : double
            The average precision at k over the input lists
    """
    if len(predicted)>k:
        predicted = predicted[:k]

    score = 0.0
    num_hits = 0.0

    for i,p in enumerate(predicted):
        if p in actual and p not in predicted[:i]:
            num_hits += 1.0
            score += num_hits / (i+1.0)

    if not actual:
        return 0.0

    return score / min(len(actual), k)

def mapk(actual, predicted, k=10):
    """
    Computes the mean average precision at k.
    This function computes the mean average prescision at k between two lists
    of lists of items.
    Parameters
    ----------
    actual : list
             A list of lists of elements that are to be predicted 
             (order doesn't matter in the lists)
    predicted : list
                A list of lists of predicted elements
                (order matters in the lists)
    k : int, optional
        The maximum number of predicted elements
    Returns
    -------
    score : double
            The mean average precision at k over the input lists
    """
    return np.mean([apk(a,p,k) for a,p in zip(actual, predicted)])

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
    params = [ [.001,.01,1,10,100],                             #alpha
            [.01,.1,1,10,50]    ,                           #regParam                            
            [3]     ,                                       #maxIter 
            [50,100]        ]                               #rank
    # params = [ [10],[1], [8]     ,[100]        ]
    params = list(itertools.product(*params))
    
    precision = []

    val1 = val.groupBy("userId").agg(F.collect_list("trackId").alias("trackId_preds"))
    # print('-----------------------------------------------------')
    # print('val')
    # print(val1.show())
    # print('-----------------------------------------------------')
    
    for i,z in zip(params,range(len(params))):
        
        als = ALS(rank = i[3], maxIter=i[2],regParam=i[1],userCol="userId", itemCol="trackId", ratingCol="count",
                    alpha = i[0], implicitPrefs = True)

        model = als.fit(train) ##train   
        ##############################################################

        ##############################################################
        users = val.select(als.getUserCol()).distinct()
        userSubsetRecs = model.recommendForUserSubset(users, 500)
        userSubsetRecs = userSubsetRecs.select("userId","recommendations.trackId")
        # print('-----------------------------------------------------')
        # print('user subset Recs')
        # print(userSubsetRecs.show(truncate=False))
        # print('-----------------------------------------------------')

        k = userSubsetRecs.join(val1,"userId")
        # print('-----------------------------------------------------')
        # print('Join')
        # print(k.show(truncate=False))
        # print('-----------------------------------------------------')
        
       

        #k = k.select('trackId_preds',"trackId").rdd
        k = k.rdd.map(lambda row: (row[1], row[2]))
        # print('-----------------------------------------------------')
        # print('Join RDD')
        # print(k.take(10))
        # print('-----------------------------------------------------')
        #k  = sc.parallelize(k)
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