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
    movie = spark.sql("SELECT title,popularity,original_language,runtime FROM movies")
    movie.createTempView("movie")
#Fetching movies which ran for longest in each language till date	
    long_run =spark.sql("SELECT first(title), max(popularity) AS popular, original_language AS language,max(runtime) AS max_runtime from movie GROUP BY language ORDER BY max_runtime DESC").show()
    long_run.coalesce(1).write.csv('output_runtime', mode='overwrite')

if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    table_name = sys.argv[2]
    main(input_keyspace,table_name)
