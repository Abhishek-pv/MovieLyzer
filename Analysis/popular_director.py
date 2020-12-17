from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as f
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
import ast





def director_Name(x):
	for i in ast.literal_eval(x[1]):
		if i['department'] == 'Directing':
			mylist = [x[0],i['name']]
			return mylist




def main(table1, table2, table3, keyspace):
	#loading data from cassandra table to dataframe.
	movies_df = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=table1, keyspace=keyspace).load()

	#selecting required columns.
	movies_df = movies_df.select("id", f.split(movies_df.release_date,'-').alias('release_year'), 'original_title')
	
	#getting year of relase
	movies_df = movies_df.withColumn("year_of_release",movies_df['release_year'].getItem(0))
	movies_df = movies_df.select("id", "year_of_release", "original_title")

	movies_df1 = movies_df.where(movies_df.id.isNull())
	
	movies_df2 = movies_df.where(movies_df.year_of_release.isNull())
	
	movies_df3 = movies_df.where(movies_df.original_title.isNull())
	

	



	#loading data from cassandra table to dataframe.
	credits_df = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=table2, keyspace=keyspace).load()

	#selecting required columns.
	credits_df = credits_df.select("id", "crew")

	credits_rdd = credits_df.rdd.map(list)

	credits_rdd = credits_rdd.map(director_Name)
	
	credits_rdd = credits_rdd.filter(lambda x : x!=None)
	credit_df = credits_rdd.toDF()
	
	
	credit_df = credit_df.withColumnRenamed('_1','id1')
	credit_df = credit_df.withColumnRenamed('_2','director_name')

	#loading data from cassandra table to dataframe.
	ratings_df = spark.read.format("org.apache.spark.sql.cassandra") \
	.options(table=table3, keyspace=keyspace).load()

	
	ratings_df = ratings_df.select("movieid", "ratings")
	ratings_df = ratings_df.where(ratings_df.ratings.isNotNull())
	ratings_df = ratings_df.groupBy("movieid").avg("ratings")


	#joinig tables

	final_df = movies_df.join(credit_df, movies_df.id == credit_df.id1)
	#final_df = movies_df.join(ratings_df, movies_df.id == ratings_df.movieId)
	finalDf = final_df.join(ratings_df, final_df.id == ratings_df.movieid)
	finalDf = finalDf.withColumnRenamed('avg(ratings)', 'ratings').cache()
	max_value = finalDf.agg({"year_of_release": "max"}).collect()[0][0]
	maximum = int(max_value)
	min_value = finalDf.agg({"year_of_release": "min"}).collect()[0][0]
	minimum = int(min_value)
	

	for i in range(minimum, maximum+1, 10):
		df = finalDf.filter((finalDf.year_of_release >= i) & (finalDf.year_of_release < i+10))
		df = df.groupBy('director_name').avg('ratings')
		df = df.withColumnRenamed('director_name', 'director_name1')
		df = df.sort(df['avg(ratings)'], ascending = False)
		df = finalDf.join(df, finalDf.director_name == df.director_name1)
		df = df.groupBy('director_name').agg(f.collect_set("original_title"), f.max("avg(ratings)"))
		df = df.sort(df['max(avg(ratings))'], ascending = False)
		df = df.withColumnRenamed('collect_set(original_title)', 'original_title')
		df = df.withColumnRenamed('max(avg(ratings))', 'ratings')
		df = df.select('director_name', concat_ws(',', df.original_title).alias('movies_directed'),'ratings')	
		print("Top 5 directors between " +str(i)+ " and " +str((i+9)))
		file_name = str(i)+'-'+str((i+9))
		df = df.limit(5)
		df.write.csv(file_name, mode = 'overwrite')
		


if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra example') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    sc = spark.sparkContext
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    table1 = sys.argv[1]
    table2 = sys.argv[2]
    table3 =sys.argv[3]
    keyspace = sys.argv[4]
    main(table1 , table2, table3, keyspace)