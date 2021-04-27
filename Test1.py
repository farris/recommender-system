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
    als = ALS(rank = 3, maxIter=2, regParam=0.01, userCol="userId", itemCol="trackId", ratingCol="count",
            implicitPrefs = True)
    model = als.fit(training)
    
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="count",
                                predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print("Root-mean-square error = " + str(rmse))

    

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    # Call our main routine
    main(spark, sc)