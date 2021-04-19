#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 21:45:20 2021

@author: farrisatif
"""


#spark-submit Test.py hdfs:/user/bm106/pub/MSD/cf_train.parquet hdfs:/user/bm106/pub/MSD/cf_validation.parquet hdfs:/user/bm106/pub/MSD/cf_test.parquet


import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

from pyspark.sql import SparkSession



def main(spark, file_path):
    
    
    # files = ['train','validation','test']
   
    # for f,z in zip(file_path,files):
        
    #     df = spark.read.parquet(str(f))
        
    #     df1 = df.repartition(1000)
        
    #     path = 'hdfs:/user/fda239/'+z+'.parquet'
        
    #     df1.write.mode("overwrite").parquet(path)
    
    # df = spark.read.parquet(str(file_path[2]))
    # df1 = df.repartition(1000)
    # df1.write.mode("overwrite").parquet('hdfs:/user/fda239/test.parquet')
    
    # df10 = spark.read.parquet(str(file_path[1]))
    # df100 = df10.repartition(1000)
    # df100.write.mode("overwrite").parquet('hdfs:/user/fda239/validation.parquet')
    
    
    
    df20 = spark.read.parquet(str(file_path[1]))
    #print(df20.columns)
    print(len(df20))
    #df200 = df20.repartition(1000000000)
    #df200.write.mode("overwrite").parquet('hdfs:/user/fda239/train.parquet')
    
    
    
    #df.createOrReplaceTempView('df')
    #df.show(2)
    

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    main(spark, sys.argv[1:])
