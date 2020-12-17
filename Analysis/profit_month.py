import sys
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import types
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *
import ast
from cassandra.cluster import Cluster
cluster_seeds = ['199.60.17.32','199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(inputs_key,table1):
#Load the data from cassandra table orders into a dataframe and create a temporary view 'movies'
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table1, keyspace=inputs_key).load()
    df.createTempView("movies")
#Select all the required fields by joining the abpove created temp view tables
    movie = spark.sql("SELECT id,title,budget,revenue,release_date FROM movies")
    movie = movie.filter((movie['budget'] != 0) & (movie['revenue'] != 0))
    movie.createTempView("movie")
#Fetching buget, revenue, year and month of release between 2000 to 2019
    movie = movie.select('id','title','budget','revenue',functions.split('release_date','-')[0].alias('years'),functions.split('release_date','-')[1].alias('months'))
    movie = movie.filter((movie['years'] >= '2000') & (movie['years'] < '2019'))
#Calculating the profit for each movie
    movie = movie.withColumn('profit', (movie['revenue'] - movie['budget']))
    #Adding an column decade by converting each year to a corresping decade
    movie.createTempView("movie_success")
#Finding the average profit of movies each month of the year
    movie_success = spark.sql("SELECT sum(profit) AS sum_profit,count(id) as count,years AS year,months AS month FROM movie_success GROUP BY year,month ORDER BY year DESC").createTempView("movie_s")
    movie_success = spark.sql("SELECT (sum_profit/count) as average_profit,year,month FROM movie_s ORDER BY year DESC")
    print(movie_success.show())
    movie_success.coalesce(1).write.csv('output_month', mode='overwrite')

if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    table1 = sys.argv[2]
    main(input_keyspace,table1)
