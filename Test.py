#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 21:45:20 2021

@author: farrisatif
"""


#spark-submit Test.py hdfs:/user/bm106/pub/MSD/cf_train.parquet hdfs:/user/bm106/pub/MSD/cf_validation.parquet hdfs:/user/bm106/pub/MSD/cf_test.parquet




# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F




def main(spark, file_path):
    

    print(file_path)



if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()


    
    main(spark, sys.argv[1])