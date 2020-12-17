import sys
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import types
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
cluster_seeds = ['199.60.17.32','199.60.17.65']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(inputs_key,table_name):
#Load the data from cassandra table orders into a dataframe and create a temporary view 'movies'
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=inputs_key).load()
    df.createTempView("movies")
#Select all the required fields by joining the abpove created temp view tables
    movie = spark.sql("SELECT vote_count,release_date FROM movies ORDER BY popularity DESC")
#Adding an additional column with release_date column split by '-'
    movie = movie.select('vote_count','release_date',functions.split('release_date','-').alias('date'))
#Keeping only the year from the previously splitted date
    movie = movie.select('vote_count',movie['date'][0].alias('year'))
#Adding an column decade by converting each year to a corresping decade
    final = movie.withColumn('decades',floor(movie['year']/10)*10)
    final.createTempView('pop_genre')
 #Find the trend of vote count has changed over time  
    final_movie = spark.sql("SELECT avg(vote_count) AS votes,decades AS decade FROM pop_genre GROUP BY decade ORDER BY decade")
#Final output dataframe contains the popular movie & genres per each decade based on the popularity
    final_movie.coalesce(1).write.csv('output_votes', mode='overwrite')

if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    table_name = sys.argv[2]
    main(input_keyspace,table_name)
