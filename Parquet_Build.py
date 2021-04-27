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
# %%
def cleaner(spark,indexed,sc):
    
    indexed.createOrReplaceTempView("ratings_idx")
    results = spark.sql("""
                            SELECT user_id, track_id, count,__index_level_0__, CAST(user_id_index AS INT) AS userId , \
                                CAST(track_id_index AS INT) AS trackId FROM ratings_idx
                                        
                            """)
    return results
#%% Function Defintions

def main(spark, sc):
    ##############################################
    sc.setLogLevel("OFF")
    spark.conf.set("spark.blacklist.enabled", "False")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    ##############################################
    file_path = ['hdfs:/user/bm106/pub/MSD/cf_train_new.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_validation.parquet',\
                'hdfs:/user/bm106/pub/MSD/cf_test.parquet']
    files = ['train','validation','test']
    ##############################################
    
    ##### HASH KEY BUILD #####
    # schemaRatings = spark.read.parquet('hdfs:/user/bm106/pub/MSD/cf_train_new.parquet')
    # schemaRatings_train = schemaRatings.sort(col('__index_level_0__'))
    # indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(schemaRatings_train) \
    #             for column in list(set(schemaRatings.columns)-set(['count'])-set(['__index_level_0__'])) ]
    # pipeline = Pipeline(stages=indexers)
    # indexed = pipeline.fit(schemaRatings)
    # path = 'hdfs:/user/fda239/hash'
    # indexed.write().overwrite().save(path) 
    ##### HASH KEY BUILD #####

    ##### PARQUET BUILD #####
    path = 'hdfs:/user/fda239/hash' 
    pipelineModel = PipelineModel.load(path)

    for f,z in zip(file_path,files):
        
            df = spark.read.parquet(f)
            df = df.sort(col('__index_level_0__'))
            df = pipelineModel.transform(df) 
            final = cleaner(spark,df,sc)

            path = 'hdfs:/user/fda239/'+z+'.parquet'

            final.write.mode("overwrite").parquet(path)
    ##### PARQUET BUILD #####

#%% Func call
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('Test').getOrCreate()
    sc = spark.sparkContext
    main(spark, sc)
