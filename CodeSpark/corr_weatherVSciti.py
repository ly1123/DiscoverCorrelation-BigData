#!/usr/bin/python 

import numpy as np
import pandas as pd
import sys
import pickle
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from MI import mutual_information 

sc = SparkContext('local')
spark = SparkSession.builder.appName("Python df").config("some-config", "some-value").getOrCreate()

#first dataset
df = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
rdd=df.rdd.map(lambda x: Row(key= ' '.join(x.key.split(' ')[:3]), birth_year=x.birth_year, end_st_id=x.end_station_id, gender=x.gerder, start_st_id=x.start_station_id, user_type=x.user_type))
df = spark.createDataFrame(rdd)
df.createOrReplaceTempView("SQLdf")
dfAgg = spark.sql('SELECT key, COUNT(*) as counts, COUNT(DISTINCT(start_st_id)) AS ststa_ucount, COUNT(DISTINCT(end_st_id)) AS ensta_ucount, AVG(birth_year) AS br, SUM(IF(user_type=="Subscriber",1,0)) AS ut_sub, SUM(IF(user_type=="Customer",1,0)) AS ut_cu FROM SQLdf GROUP BY key')

#second data set
df1 = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
rdd1 = df1.rdd.map(lambda x: Row(key=' '.join(x.key.split(' ')[:3]), spd=x.spd, visb=x.visb, temp=x.temp, prcp=x.prcp))
df1 = spark.createDataFrame(rdd1)
df1.createOrReplaceTempView("SQLdf1")
dfAgg1 = spark.sql('SELECT key, COUNT(*) as counts1, AVG(spd) as spd_avg, MAX(spd) AS spd_max, AVG(visb) AS visb_avg, MAX(visb) AS visb_max, AVG(temp) AS temp_avg, MAX(temp) AS temp_max, AVG(prcp) as prcp_avg, MAX(prcp) AS prcp_max FROM SQLdf1 GROUP BY key')

dfAgg = dfAgg.toPandas().set_index('key')
variable = list(dfAgg.columns)
dfAgg1 = dfAgg1.toPandas().set_index('key')
variable1 = list(dfAgg1.columns)
print(variable)
print(variable1)

df_join = dfAgg.join(dfAgg1, how='inner')
print(df_join.shape)
df_join.to_csv('data/join_citiVSweather.csv')
corr_p = np.zeros((len(variable), len(variable1)))
corr_mi = np.zeros((len(variable), len(variable1)))
for i,v in enumerate(variable):
	if np.mean(pd.isnull(df_join[v].values)) > 0.8 or np.mean(df_join[v]==0)>0.9:
		print('Over 80% or 90% missing values or 0 in '+v)
		continue
	for j,v1 in enumerate(variable1):
		if np.mean(pd.isnull(df_join[v1].values)) > 0.8 or np.mean(df_join[v1]==0)>0.9:
			print('Over 80% or 90% missing values or 0 in '+v1)
			continue
		df_co = df_join[[v, v1]].values
		corr_p[i, j] = np.corrcoef(df_co.T)[0, 1]
		corr_mi[i ,j] = mutual_information(df_co[:,0], df_co[:,1], 1, 1, 30)
		if abs(corr_p[i, j]) > 0.3:
			print(v+' VS '+v1 )
			print(corr_p[i,j])
			print(corr_mi[i,j])
np.save('data/corrP_citiVSweather.npy', corr_p)
np.save('data/corrMI_citiVSweather.npy', corr_mi)

