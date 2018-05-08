#!/usr/bin/python 

import numpy as np
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from mappers import mapper_311, mapper_collision, mapper_property, mapper_weather, mapper_citibike

sc = SparkContext('local')
spark = SparkSession.builder.appName("Python df").config("some-config", "some-value").getOrCreate()

# data for transform zipcode 
# dict: with key=grid and value=polygons(index) of corresponding zip code
with open('grid_zipcode_dict.pickle', 'rb') as f:
	grid_zipcode_dict = pickle.load(f)
# bound information for grids
with open('bound_zipcode.pickle', 'rb') as f:
	bound_zipcode = pickle.load(f)
# list of polygons
with open('polys_zipcode.pickle', 'rb') as f:
	polys_zipcode = pickle.load(f)	
# dict: with key=polygons and value=zip code
with open('polys_zipcode_dict.pickle', 'rb') as f:
	polys_zipcode_dict = pickle.load(f)
zipcode = (grid_zipcode_dict, polys_zipcode, polys_zipcode_dict, bound_zipcode)

'''
#citi_bike
rdd = sc.textFile(sys.argv[1])
rdd = rdd.filter(lambda x: 'start' not in x.lower()) #remove header of citi bike
rdd = rdd.map(lambda x: mapper_citi(x, zipcode)).filter(lambda x: int(x['key'].split(' ')[0])>2005 and int(x['key'].split(' ')[0])<2018) #structure data and filter out outliers
df = spark.createDataFrame(rdd)
print(df.show())
df.write.format('com.databricks.spark.csv').save('/user/tx443/citi_preprocessed.csv',header = 'true')
'''

'''
#weather
rdd = sc.textFile(sys.argv[1])
rdd = rdd.filter(lambda x: 'date' not in x.lower()) #remove header of weather
rdd = rdd.map(lambda x: mapper_weather(x, zipcode)).filter(lambda x: int(x['key'].split(' ')[0])>2005 and int(x['key'].split(' ')[0])<2018)
df = spark.createDataFrame(rdd)
df.write.format('com.databricks.spark.csv').save('/user/tx443/weather_preprocessed.csv',header = 'true')
'''

'''
#property
rdd = sc.textFile(sys.argv[1])
rdd = rdd.filter(lambda x: 'bble' not in x.lower()) #remove header of property
rdd = rdd.map(lambda x: mapper_property(x, zipcode)).filter(lambda x: int(x['key'].split(' ')[0])>2005 and int(x['key'].split(' ')[0])<2018)
df = spark.createDataFrame(rdd)
print(df.show())
df.write.format('com.databricks.spark.csv').save('/user/sz2396/property_preprocessed.csv',header = 'true')
'''

'''
#collision
rdd = sc.textFile(sys.argv[1])
rdd = rdd.filter(lambda x: 'date' not in x.lower()) #remove header of collision
rdd = rdd.map(lambda x: mapper_collision(x, zipcode)).filter(lambda x: int(x['key'].split(' ')[0])>2005 and int(x['key'].split(' ')[0])<2018)
df = spark.createDataFrame(rdd)
print(df.show())
df.write.csv('/user/sz2396/collision_preprocessed.csv')
df.write.format('com.databricks.spark.csv').save('/user/sz2396/collision_preprocessed.csv',header = 'true')
'''

'''
#311
rdd = sc.textFile(sys.argv[1])
rdd = rdd.filter(lambda x: 'descriptor' not in x.lower()) #remove header of 311
rdd = rdd.map(lambda x: mapper_311(x)).filter(lambda x: int(x['key'].split(' ')[0])>2005 and int(x['key'].split(' ')[0])<2018)
df = spark.createDataFrame(rdd)
df.write.csv('/user/sz2396/property_preprocessed.csv')
df.write.format('com.databricks.spark.csv').save('/user/sz2396/property_preprocessed.csv',header = 'true')
'''

'''
#crime
rdd = sc.textFile(sys.argv[1])
rdd = rdd.filter(lambda x: '' not in x.lower()) #remove header of 311
rdd = rdd.map(lambda x: mapper_(x)).filter(lambda x: int(x['key'].split(' ')[0])>2005 and int(x['key'].split(' ')[0])<2018)
df = spark.createDataFrame(rdd)
df.write.csv('/user/tx443/crime_preprocessed.csv')
df.write.format('com.databricks.spark.csv').save('/user/tx443/cirme_preprocessed.csv',header = 'true')
'''
