#Analysis of Average Ratings for movies by users and loading the data into cassandra 

import sys
import os
from cassandra.cluster import *
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('co relation code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext



# ETL on movies dataset for release_date
@functions.udf(returnType=types.StringType())
def extract_year(d) :

    date1 = str(d).split("-")
    return int(date1[0])



def main(input_keyspace):
  
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

    #Reading rows from movies_metadata cassandra table
    table = "movies"
    movies_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = table, keyspace = input_keyspace).load()
    movies_df = movies_df.select("id", "title","release_date")
    movies_df.cache()
    movies_df.show(10)
    print("movies df count= ",movies_df.count())


    #Reading rows from ratings cassandra table
    table = "ratings"
    ratings_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = table, keyspace = input_keyspace).load()
    ratings_df = ratings_df.select("movieid","ratings")
    ratings_df.cache()
    print("ratings count=",ratings_df.count())
      

    #Calculate Average Ratings in the ratings dataset for corresponding movies
    avg_df = ratings_df.groupby(ratings_df['movieid']).agg(functions.avg(ratings_df['ratings']).alias("average_rating"))
    avg_df.show(10)
    
    #Creating temp tables for movies df and the joined avg ratings df
    avg_df.createOrReplaceTempView("average")
    movies_df.createOrReplaceTempView("movies")
    print("After average count= ",avg_df.count())

    #Join movies_df and avg_df 
   
    join_df = spark.sql("SELECT * from average a JOIN  movies m ON a.movieid = m.id ")
    join_df.show(10)
    print(join_df.count())
    
    #ETL on reales_date to extract only year
    temp_df = join_df.withColumn('year_of_release',extract_year(join_df['release_date']))
    
    final_df = temp_df.select(temp_df.movieid, temp_df.title,temp_df.year_of_release,temp_df.average_rating)
    final_df.show(10)

    #Writing the average ratings to cassandra table
    #Create table Script
    #CREATE TABLE avg_ratings (movieid int (PRIMARY KEY), title text, year_of_release int ,average_ratings double) 
    table = "avg_ratings"
    final_df.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=input_keyspace).save()


#Main Function
if __name__ == '__main__':
    keyspace = sys.argv[1]
    main(keyspace)

