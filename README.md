### Authors: Farris Atif & Zafir Momin
# About

This repository contains an implementation of Spark's built in implicit ALS matrix factorization , allowing us to create an implicit reccomender system using 
the Million Song Dataset https://www.kaggle.com/c/msdchallenge. The 200+ gb dataset is housed on NYU's High Performance Computing Cluster (HPC) : Peele , where all computation was performed. Lastly, this was completed for credit as part of the final project for DS-GA 1004 (Big Data) @ NYU CDS.


# GITHUB ORGANIZATION 


The following files were run sequentially to obtain the final results from the ALS Model (ie. 500 recommendations per user)

## ——branch MAIN: ALS MODEL———————————————

1) Build_Hash.py : .py file that creates a uniform integer hash key for the train, test, and validation sets. This key is then saved locally on HDFS

2) Parquet_Build.py: .py file that loads in the uniform hash key from HDFS, applies it to each of the datasets, and then writes the new files back out to our local HDFS

3) GridSearch_All.py: .py file that performs grid search on the ALS model

4) GridSearchFinal: Folder that contains the results of our grid search and the corresponding Jupiter notebook

5) FinalModel.py: .py file that contains our final model run, with the optimal hyper parameters (running to a high level of iterations)

## ——branch MAIN: EXTENSION———————————————

6) Subsample.py: .py file that subsamples from train & test user/track/count data (.5%)

7) Lenskit_Extension.ipynb: Extension results


