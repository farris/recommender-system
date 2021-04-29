#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''

#spark-submit WriteFiles.py train_new/validation/test


import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

from pyspark.sql import SparkSession

def main(spark, file_type):
    df = spark.read.parquet("hdfs:/user/bm106/pub/MSD/cf_" + str(file_type) + ".parquet")
        
#     df = df.repartition(1000)
    
    
    
    
    
    
    
    
    path = 'hdfs:/user/zm2114/pub/cf_'+file_type+'.parquet'   
    df.write.mode("overwrite").parquet(path)    
    print("File written to " + path)
    
    
                            
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    main(spark, sys.argv[1])