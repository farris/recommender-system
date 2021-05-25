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


# DSGA1004 - BIG DATA
## Final project
- Prof Brian McFee (bm106)

*Handout date*: 2021-04-08

*Submission deadline*: 2021-05-18


# Overview

In the final project, you will apply the tools you have learned in this class to solve a realistic, large-scale applied problem.
There are two options for how you go about this:

1. [**The default**](#option-1-recommender-system): build and evaluate a collaborative-filter based recommender system.  The details and requirements of this option are described below.
2. [**Propose your own**](#option-2-choose-your-own): if you already have sufficient experience with recommender systems and want to try something different, you can propose your own project.  See below on how to go about this.


In either case, you are encouraged to work in **groups of up to 4 students**.

If you're taking the default option:

- Groups of 1--2 will need to implement one extension (described below) over the baseline project for full credit.
- Groups of 3--4 will need to implement two extensions for full credit.

# Option 1: Recommender system
## The data set

In this project, we'll use the [Million Song Dataset](http://millionsongdataset.com/) (MSD) collected by 
> Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. 
> The Million Song Dataset. In Proceedings of the 12th International Society
> for Music Information Retrieval Conference (ISMIR 2011), 2011.

The MSD consists of (you guessed it) one million songs, with metadata (artist, album, year, etc), tags, partial lyrics content, and derived acoustic features.  You need not use all of these aspects of the data, but they are available.
The MSD is hosted in NYU's HPC environment under `/scratch/work/courses/DSGA1004-2021/MSD`.

The user interaction data comes from the [Million Song Dataset Challenge](https://www.kaggle.com/c/msdchallenge)
> McFee, B., Bertin-Mahieux, T., Ellis, D. P., & Lanckriet, G. R. (2012, April).
> The million song dataset challenge. In Proceedings of the 21st International Conference on World Wide Web (pp. 909-916).

The interaction data consists of *implicit feedback*: play count data for approximately one million users.
The interactions have already been partitioned into training, validation, and test sets for you, as described below.

On Peel's HDFS, you will find the following files in `hdfs:/user/bm106/pub/MSD`:

  - `cf_train.parquet`
  - `cf_validation.parquet`
  - `cf_test.parquet`

Each of these files contains tuples of `(user_id, count, track_id)`, indicating how many times (if any) a user listened to a specific track.
For example, the first few rows of `cf_train.parquet` look as follows:

|    | user_id                                  |   count | track_id           |
|---:|:-----------------------------------------|--------:|:-------------------|
|  0 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRIQAUQ128F42435AD |
|  1 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRIRLYL128F42539D1 |
|  2 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       2 | TRMHBXZ128F4238406 |
|  3 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRYQMNI128F147C1C7 |
|  4 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRAHZNE128F9341B86 |

These files are also available under `/scratch/work/public/MillionSongDataset/` if you want to access them from outside of HDFS.


## Basic recommender system [80% of grade]

Your recommendation model should use Spark's alternating least squares (ALS) method to learn latent factor representations for users and items.
Be sure to thoroughly read through the documentation on the [pyspark.ml.recommendation module](https://spark.apache.org/docs/2.4.7/ml-collaborative-filtering.html) before getting started.

This model has some hyper-parameters that you should tune to optimize performance on the validation set, notably: 

  - the *rank* (dimension) of the latent factors, and
  - the regularization parameter.

### Evaluation

Once your model is trained, you will need to evaluate its accuracy on the validation and test data.
Scores for validation and test should both be reported in your final writeup.
nce your model is trained, evaluate it on the test set  
Evaluations should be based on predictions of the top 500 items for each user, and report the ranking metrics provided by spark.
Refer to the [ranking metrics](https://spark.apache.org/docs/2.4.7/mllib-evaluation-metrics.html#ranking-systems) section of the Spark documentation for more details.

The choice of evaluation criteria for hyper-parameter tuning is up to you, as is the range of hyper-parameters you consider, but be sure to document your choices in the final report.
As a general rule, you should explore ranges of each hyper-parameter that are sufficiently large to produce observable differences in your evaluation score.


If you like, you may also use additional software implementations of recommendation or ranking metric evaluations, but please cite any additional software you use in the project.
