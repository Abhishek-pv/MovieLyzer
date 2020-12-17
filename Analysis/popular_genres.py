import sys,os,ast
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types,Row
from cassandra.cluster import Cluster
from pyspark.sql.functions import split, regexp_replace

spark = SparkSession.builder.appName('Cassandra Dataframe').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

#Function to get each genres from the list 
def getGenre(x):
    y = x[0]
    z = y[1:len(y)-1]
    return [z.split(','),x[1]]

#Function to extract the year from date format
def extract_year(d) :
    date1 = str(d[1]).split("-")
    yield d[0],int(date1[0])

#Function that returns the decade
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


def main(keyspace,table,output):
    
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    #Loading the data from Cassandra table 
    movies_df = spark.read.format("org.apache.spark.sql.cassandra").options(table = table, keyspace = keyspace).load()

    req_df = movies_df.select('genres','release_date')
    yearwise_df = req_df.rdd.flatMap(extract_year).toDF()
    yearwise_df = yearwise_df.withColumnRenamed('_1','genres')
    yearwise_df = yearwise_df.withColumnRenamed('_2','year')

    df_decade = yearwise_df.withColumn("decade",cal_decade(yearwise_df['year'])).select('genres','decade')

    rdd1 = df_decade.rdd.map(list)
    #Extracting individual genres of the movies
    rdd2 = rdd1.map(getGenre)
    genre_df = rdd2.toDF().cache()

    genre_df = genre_df.withColumnRenamed('_1','genres1')
    genre_df = genre_df.withColumnRenamed('_2','decade')
    
    genre_df = genre_df.select(functions.explode(genre_df["genres1"]).alias('genre'),'decade')

    #Counting the number of genres in eacch decade
    genre_df = genre_df.groupBy("genre","decade").agg(functions.count("genre").alias("count"))
    genre_df.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    output = sys.argv[3]
    main(keyspace,table,output)

