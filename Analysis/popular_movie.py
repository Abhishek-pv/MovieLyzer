#Analysis of Popular Movies in a decade

import sys
import os
from cassandra.cluster import *
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('co relation code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext



@functions.udf(returnType=types.StringType())
def cal_decade(year) :
    diff = year%10
    if diff == 0:
        start_year = year
    else:
        start_year = year-diff
    end_year = start_year+10
    decade = str(start_year)+"-"+str(end_year)
    return decade 



def main(input_keyspace,output):	


    cluster_seeds = ['199.60.17.32', '199.60.17.65']

    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

    #Reading rows from avg_ratings cassandra table
    table = "avg_ratings"
    avg_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = table, keyspace = input_keyspace).load()
    avg_df.cache()
    
    #Creating Temperory Table for the avg_df
    avg_df.createOrReplaceTempView("avg_ratings")
    #Arrange the records in the avg_ratings table in asc order of release_year
    df_decade = spark.sql("SELECT * from avg_ratings order by year_of_release")
    
    #Calculating decade in which corresponding movie released
    df_decade = df_decade.withColumn("decade",cal_decade(df_decade['year_of_release']))
    #df_decade.show(30)


    #Finding the most popular movie in a particular decade (using average ratings given by users)
    popular_movie = df_decade.groupby(df_decade['decade']).agg(functions.max(df_decade['average_rating']).alias("highest_rating"))    
    #popular_movie.show(20)
    
    #Join the popular movie in decade to get title and tie between the records
    condition = (popular_movie.decade == df_decade.decade) & (popular_movie.highest_rating == df_decade.average_rating)
    final_df = popular_movie.join(df_decade,condition).select(df_decade['title'],popular_movie['highest_rating'],df_decade['decade'])
    final_df.show(30)


    #Write output in csv file
    final_df.write.csv(output,mode='overwrite')



if __name__ == '__main__':
    keyspace = sys.argv[1]  #keyspace in cassandra
    output = sys.argv[2]    #output directory
    main(keyspace,output)

