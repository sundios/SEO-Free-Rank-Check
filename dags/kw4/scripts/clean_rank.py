#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 22:10:03 2019

@author: konradburchardt
"""

import pandas as pd
import numpy as np
import os
import glob


#Opening file with all ranks

path = '/usr/local/airflow/dags/kw4/' ## airflow
#path = '/Users/konradburchardt/Desktop/docker-airflow/dags'  ##local
os.chdir(path)
files = sorted(glob.glob(os.path.join(path,'kw4.csv')))

#global file with all rankings
for f in files:
    print(f)
    df = pd.read_csv(f,index_col=False)
    
    #Slicing and selecting the columns
    df_clean = pd.DataFrame(df, columns = ['Keyword','Rank','Date'])
    

#opening new rank

path2 = '/usr/local/airflow/dags/kw4/rankings/' ## airflow
#path2 = '/Users/konradburchardt/Desktop/docker-airflow/dags/rankings'  ##local
new_rank = sorted(glob.glob(os.path.join(path2,'*.csv')))


for nr in new_rank:
    print(nr)
    #open and read the new rank
    new_rank2 = pd.read_csv(nr,index_col=False)
    
    #Slicing and selecting the columns
    nr_clean = pd.DataFrame(new_rank2, columns = ['Keyword','Rank','Date'], index=[0])
    
    ranks = df.append(nr_clean)
    

ranks.reset_index(drop=True, inplace=True)

ranks = pd.DataFrame(ranks, columns = ['Keyword','Rank','Date'])

print(ranks)

#exporting to CSV
export_csv = ranks.to_csv (r'/usr/local/airflow/dags/kw4/kw4.csv', index =True,index_label = 'id', header=True)

#test local
#export_csv = ranks.to_csv (r'ranks.csv', index =True, index_label = 'id', header=True)







