#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''

# And pyspark.sql to get the spark session
#Set up a spark context

from pyspark import SparkContext,  SparkConf

def main(spark, sc):
    sc.setLogLevel("WARN")
    ratings = sc.textFile('implicit.csv').map(lambda x: x.replace('"',"")) \
            .map(lambda x:x.split(";"))\
            .map(lambda x:(int(x[0]), str(x[1]), int(x[2])))

    print(ratings.take(10))

    print('------------------------------------------------------------------------')
    #Taking all distinct books
    isbns=ratings.map(lambda x:x[1]).distinct()
    
    #Associates an integer with each unique isbn.
    isbns_with_indices=isbns.zipWithIndex() 
    print('#(isbn, index)')
    print(isbns_with_indices.take(10))
    print('------------------------------------------------------------------------')
    
    #sets isbn as the key
    reordered_ratings = ratings.map(lambda x:(x[1], (x[0], x[2]))) 
    print('#(isbn, userid, rating)')
    print(reordered_ratings.take(10))
    print('------------------------------------------------------------------------')
    
    
    joined = reordered_ratings.join(isbns_with_indices) #joins with indexes 
    print('#(isbn, ((userid, rating), isbn-id-integer))')
    print(joined.take(10))
    print('------------------------------------------------------------------------')
        
    ratings_int_nice = joined.map(lambda x: (x[1][0][0], x[1][1], x[1][0][1]))
    print('#(user id, isbn-id-integer, rating)')
    print(ratings_int_nice.take(10))
    print('------------------------------------------------------------------------')
    
    ratings_ones = ratings_int_nice.map(lambda x:(x[0], x[1], 1))
    print(ratings_ones.take(10))

#     from pyspark.mllib.recommendation import ALS
#     model=ALS.trainImplicit(ratings_ones, rank=5, iterations=3, alpha=0.99)

#     #Filter out all the  id of all books rated by user id = 8. 
#     users_books = ratings_ones.filter(lambda x: x[0] is 8).map(lambda x:x[1])
#     books_for_them = users_books.collect() #Collect this as a list
    
#     print(books_for_them)
#     print('------------------------------------------------------------------------')
#     unseen = isbns_with_indices.map(lambda x:x[1]) \
#                             .filter(lambda x: x not in books_for_them) \
#                             .map(lambda x: (8, int(x)))
#     print(unseen.take(10))
    
    
if __name__ == "__main__":

    conf = SparkConf().setAppName("implicitALS")
    sc = SparkContext(conf=conf)
    main(conf, sc)