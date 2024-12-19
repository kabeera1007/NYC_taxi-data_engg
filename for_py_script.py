#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()


df_green = spark.read.parquet(input_green)


df_yellow = spark.read.parquet(input_yellow)


df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]

df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)



df_result = (
    df_trips_data
    .withColumn("revenue_month", F.date_trunc('month', F.col('pickup_datetime')))
    .groupBy(
        "PULocationID",
        "revenue_month",
        "service_type"
    )
    .agg(
        F.sum('fare_amount').alias('revenue_monthly_fare'),
        F.sum('extra').alias('revenue_monthly_extra'),
        F.sum('mta_tax').alias('revenue_monthly_mta_tax'),
        F.sum('tip_amount').alias('revenue_monthly_tip_amount'),
        F.sum('tolls_amount').alias('revenue_monthly_tolls_amount'),
        F.sum('improvement_surcharge').alias('revenue_monthly_improvement_surcharge'),
        F.sum('total_amount').alias('revenue_monthly_total_amount'),
        F.sum('congestion_surcharge').alias('revenue_monthly_congestion_surcharge'),
        F.avg('passenger_count').alias('avg_montly_passenger_count'),
        F.avg('trip_distance').alias('avg_montly_trip_distance')
    )
)

# Write the result to a Parquet file
df_result.coalesce(1).write.parquet(output, mode='overwrite')


