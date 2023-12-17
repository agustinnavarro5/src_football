import math
import numpy as np
import pandas as pd
from itertools import combinations
from geopy import distance
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, window, max as pyspark_max, col



spark = SparkSession.builder.config("spark.jars", "./postgresql-42.7.1.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()

# Database connection properties
postgres_url = "jdbc:postgresql://127.0.0.1:6432/src_football"
connection_properties = {
    "user": "src_football",
    "password": "src_football",
    "driver": "org.postgresql.Driver"
}

def read_table(table_name):

    # Read data from PostgreSQL table into a DataFrame
    df = spark.read \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", table_name) \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .load()

    return df


def compute_five_most_intense_minutes(df):

    # Calculate interval start and sum count per minute
    grouped_by_five_minutes_data = df \
        .withColumn("interval_start", F.date_trunc("minute", df["date_minute"]) - (F.minute(df["date_minute"]) % 5) * F.expr("INTERVAL 1 minute")) \
        .groupBy("match_id", "player_id", "interval_start") \
        .agg(F.sum("count_per_minute").alias("sum_count_per_minute")) \
        .orderBy("match_id", "player_id", "interval_start")

    # Calculate row number based on sum_count_per_minute partitioned by match_id and player_id
    windowSpec = Window.partitionBy("match_id", "player_id").orderBy(F.col("sum_count_per_minute").desc())
    grouped_data = grouped_by_five_minutes_data \
        .withColumn("rn", F.row_number().over(windowSpec)) \
        .filter(F.col("rn") == 1)

        # Show the resulting DataFrame
    grouped_data.show()

@pandas_udf('double')
def avg_distance_across_points(lat: pd.Series, lon: pd.Series) -> float:
    points = list(zip(lat, lon))
    if len(points) <= 1:
        d = 0
    else:
        d = np.mean([distance.distance(p1, p2).km for p1, p2 in combinations(points, 2)])
    return d

def compute_two_minutes_with_highest_spread(df):
    # compute spread metric
    df = df\
    .groupby('match_id', 'date_time')\
    .agg(avg_distance_across_points('point_x', 'point_y').alias('avg_distance'))

    # Convert 'date_time' column to TimestampType
    df = df.withColumn('date_time', col('date_time').cast('timestamp'))
    # Group by match_id and window based on 2-minute intervals, calculate the max average distance
    df = (
        df
        .groupBy('match_id', window('date_time', '2 minutes'))
        .agg({'avg_distance': 'avg'})
        .groupBy('match_id')
        .agg(pyspark_max('avg(avg_distance)').alias('max_avg_distance'))
    )


table_name = "public_marts.dim_src__matches_participation_data"
df = read_table(table_name)
compute_five_most_intense_minutes(df)


table_name = "public_marts.dim_src__matches_data_points"
df = read_table(table_name)
compute_two_minutes_with_highest_spread(df)
spark.stop()