# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 01:46:11 2019

@author: bda
"""
#hadoop fs -get hdfs:///abd/output_top5_1/part-00000 output_top5.txt


import pandas as pd
import re
top5_df = pd.read_csv('output_top5.txt', sep="\t", header=None)

#data.columns = ["a", "b", "c", "etc."]
years = [i for i in top5_df[0]]
perday_pmvalues = []
for i in range(len(top5_df)):
    temp = str(top5_df[1][i]).strip("[]")
    temp = re.sub('"','',temp)
    temp = temp.split(',')
    pm_values = []
    for j in temp:
        j = j.strip().split()
        pm_values.append(float(j[0]))
    perday_pmvalues.append(pm_values)
        
df = pd.DataFrame(perday_pmvalues, columns = ['1st', '2nd', '3rd', '4th', '5th'], index = years)

df.plot.bar()

        

