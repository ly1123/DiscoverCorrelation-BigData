#!/usr/bin/python 

import numpy as np
import pandas as pd
import sys
import pickle
from pyspark.sql import SparkSession
from pyspark import SparkContext
import time
from pyspark.sql import Row
from pyspark.sql import functions as F
from MI import mutual_information 

sc = SparkContext('local')
spark = SparkSession.builder.appName("Python df").config("some-config", "some-value").getOrCreate()

#first dataset
df = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
rdd= df.rdd.map(lambda x: Row(key= ' '.join(x['Year_Zip'].split(',')), bble=x.BBLE, b=x.B, block=x.BLOCK, lot=x.LOT, bldgcl=x.BLDGCL, taxclass=int(x.TAXCLASS[0]), ltf=x.LTFRONT, ltd=x.LTDEPTH, fullval=x.FULLVAL, avland=x.AVLAND, avtot=x.AVTOT, exland=x.EXLAND, extot=x.EXTOT, bldf=x.BLDFRONT, bldd=x.BLDDEPTH))
df = spark.createDataFrame(rdd)
df.createOrReplaceTempView("SQLdf")
dfAgg = spark.sql('SELECT key, COUNT(*) as counts, COUNT(DISTINCT(bble)) bble_ucount, SUM(IF(b=1,1,0)) AS b_1, SUM(IF(b=2,1,0)) AS b_2, SUM(IF(b=3,1,0)) AS b_3, SUM(IF(b=4,1,0)) AS b_4, SUM(IF(b=5,1,0)) AS b_5, COUNT(DISTINCT(block)) AS block_ucount, COUNT(DISTINCT(lot)) AS lot_ucount, SUM(IF(taxclass=1,1,0)) AS taxc_1, SUM(IF(taxclass=2,1,0)) AS taxc_2, SUM(IF(taxclass=3,1,0)) AS taxc_3, SUM(IF(taxclass=4,1,0)) AS taxc_4, AVG(ltf) AS ltf_avg, AVG(ltd) AS ltd_avg, AVG(fullval) AS fval_avg, MAX(fullval) AS fval_max, MIN(fullval) as fval_min, AVG(avland) AS avland_avg, MAX(avland) AS avland_max, MIN(avland) AS avland_min, AVG(avtot) AS avtot_avg, MAX(avtot) AS avtot_max, MIN(avtot) AS avtot_min, AVG(exland) as exland_avg, AVG(extot) as extot_avg, AVG(bldf) AS bldf_avg, AVG(bldd) AS bldd_avg FROM SQLdf GROUP BY key')

#second data set
dfAgg1 = pd.read_csv('data/census_preprocessed.csv', index_col='key')
variable = list(dfAgg.columns)
variable1 = list(dfAgg1.columns)

df_join = dfAgg.join(dfAgg1, how='inner')
print(df_join.shape)
df_join.to_csv('data/join_propVScensus.csv')
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
		try:
			corr_p[i, j] = np.corrcoef(df_co.T)[0, 1]
			corr_mi[i ,j] = mutual_information(df_co[:,0], df_co[:,1], 1, 1, 30)
		except:
			corr_p[i,j] = 0
			corr_mi[i,j] = 0
		if abs(corr_p[i, j]) > 0.5:
			print(v+' VS '+v1 )
			print(corr_p[i,j])
			print(corr_mi[i,j])
np.save('data/corrP_propVScensus.npy', corr_p)
np.save('data/corrMI_propVScensus.npy', corr_mi)

