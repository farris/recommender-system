#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 20 17:23:38 2021

@author: farrisatif
"""

import pandas as pd
import random
import copy
df = pd.read_csv('kaggle_visible_evaluation_triplets.txt', delimiter = "\t",header = None)
df.columns = ['user_id','track_id','count']

df['user_id'] = df['user_id'].astype(str)
df['track_id'] = df['track_id'].astype(str)
df['count'] = df['count'].astype(int)


u = df['user_id'].unique()
t = df['track_id'].unique()
c = df['count'].unique()
# %%
hash1 = [random.randint(0,len(c)) for i in range(30)]
take_out = c[hash1]  ## these are the counts we will take out


df_train = copy.deepcopy(df)    ## new training set



df_test = df_train.loc[df_train['count'].isin(take_out), ['user_id','track_id']] ##use this to test model
actual = df_train.loc[df_train['count'].isin(take_out)]

df_train.loc[df_train['count'].isin(take_out), 'count'] = '' ## new matrix with values taken out 
 

# %%


df_test.to_csv('testing_set.csv')

actual.to_csv('loss_eval.csv')

df_train.to_csv('train.csv')

# %%

TRAIN = pd.read_csv('train.csv')
