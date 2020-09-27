# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 11:08:14 2019

@author: bda
"""


#Import Dependencies
from mrjob.job import MRJob
from statistics import median

class MRWordCount(MRJob):

  def mapper(self,_,lines):
    id, sensor_id, location, latitude, longitude, timestamp, p1, p2 = lines.split(',')
    if(sensor_id != 'sensor_id'):
        day = timestamp[0:10]
        pm = p1
    
        yield day, str(pm) +' ' + latitude + ' ' + longitude

  def reducer(self,key,values):
      list1 = list(values)
      list1.sort(reverse = True)
      
      yield key, list1[0:5]

if __name__ == '__main__':
    MRWordCount.run()