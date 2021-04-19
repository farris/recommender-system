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
    
    
    
    i = 0
    print('------------------------')
    for f in file_path:
        if i == 1:
            break
        df = spark.read.parquet(str(f))
        df.createOrReplaceTempView('cf_train')
        query10 = spark.sql(""" SELECT * from cf_train
                                LIMIT 10
                                    """)
        query10.show()
    
  
        i+=1

if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    main(spark, sys.argv[1:])
