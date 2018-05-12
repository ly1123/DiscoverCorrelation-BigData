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


#taxi dataset
df_taxi = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
rdd = df_taxi.rdd.map(lambda x: Row(key=' '.join(x.key.split(' ')[:3]), total_amt = x.total_amt, drh =x.drh, passenger_count = x.passenger_count, trip_d=x.trip_d, pay_type= x.pay_type, fare_amt =x.fare_amt, tip_amt = x.tip_amt, tolls_amt = x.tolls_amt))
schema = StructType([
        StructField("key", StringType(), True),
        StructField("total_amt", FloatType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("drh", FloatType(), True),
        StructField("trip_d", FloatType(), True),
        StructField("pay_type", FloatType(), True),
        StructField("fare_amt", FloatType(), True),
        StructField("tip_amt", FloatType(), True),
        StructField("tolls_amt", FloatType(), True)])
df_taxi = spark.createDataFrame(rdd)
df_taxi.createOrReplaceTempView("SQLdf")
dfAgg = spark.sql('SELECT key, COUNT(*) as counts, AVG(passenger_count) as avg_passenger, SUM(passenger_count) as passenger_count, AVG(trip_d) as avg_distance, SUM(trip_d) as sum_distance, AVG(tip_amt) as avg_tip, SUM(tip_amt) as sum_tip, SUM(tolls_amt) as sum_tolls, AVG(total_amt) as avg_total_amt, SUM(total_amt) as total_amt FROM SQLdf GROUP BY key')
keys = dfAgg.select("key").rdd.map(lambda x: x['key']).collect()


#second data set
df1 = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
rdd1 = df1.rdd.map(lambda x: Row(key=' '.join(x.key.split(' ')[:3]), spd=x.spd, visb=x.visb, temp=x.temp, prcp=x.prcp))
df1 = spark.createDataFrame(rdd1)
df1.createOrReplaceTempView("SQLdf1")
dfAgg1 = spark.sql('SELECT key, COUNT(*) as counts1, AVG(spd) as spd_avg, MAX(spd) AS spd_max, AVG(visb) AS visb_avg, MAX(visb) AS visb_max, AVG(temp) AS temp_avg, MAX(temp) AS temp_max, AVG(prcp) as prcp_avg, MAX(prcp) AS prcp_max FROM SQLdf1 GROUP BY key')
keys1 = dfAgg1.select("key").rdd.map(lambda x: x['key']).collect()

df_join = dfAgg.join(dfAgg1, how='inner')
print(df_join.shape)
# inner join table 
df_join.to_csv('data/join_taxiVSweather.csv')
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
		corr_mi[i ,j] = mutual_information(df_co[:,0], df_co[:,1], 1, 1, 10)
		if abs(corr_p[i, j]) > 0.1:
			print(v+' VS '+v1 )
			print(corr_p[i,j])
			print(corr_mi[i,j])
np.save('data/corrP_taxiVSweather.npy', corr_p)
np.save('data/corrMI_taxiVSweather.npy', corr_mi)
