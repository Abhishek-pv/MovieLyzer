import sys,os,ast
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types,Row
from cassandra.cluster import Cluster

spark = SparkSession.builder.appName('Cassandra Dataframe').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def getValues(x):
    mylist = []
    for i in ast.literal_eval(x[1]):
        mylist.append(i['name'])
    mylist1 = mylist
    mylist = []
    return x[0],mylist1


def main(inputs,keyspace,table):

    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    movies_schema = types.StructType([
    types.StructField('adult', types.BooleanType()),
    types.StructField('belongs_to_collection', types.StringType()),
    types.StructField('budget', types.DoubleType()),
    types.StructField('genres', types.StringType()),
    types.StructField('homepage', types.StringType()),
    types.StructField('id', types.IntegerType(), False),
    types.StructField('imdb_id', types.StringType(), False),
    types.StructField('original_language', types.StringType()),
    types.StructField('original_title', types.StringType()),
    types.StructField('overview', types.StringType()),
    types.StructField('popularity', types.DoubleType()),
    types.StructField('poster_path', types.StringType()),
    types.StructField('production_companies', types.StringType()),
    types.StructField('production_countries', types.StringType()),
    types.StructField('release_date', types.DateType()),
    types.StructField('revenue', types.DoubleType()),
    types.StructField('runtime', types.IntegerType()),
    types.StructField('spoken_languages', types.StringType()),
    types.StructField('status', types.StringType()),
    types.StructField('tagline', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('video', types.BooleanType()),
    types.StructField('vote_average', types.DoubleType()),
    types.StructField('vote_count', types.IntegerType()),
])


    movies_df = spark.read.csv(inputs,schema=movies_schema,header=True)
    
    data_df = movies_df.select('budget','genres','id','original_language','popularity','production_companies',
	'production_countries','release_date','revenue','runtime','title','vote_average','vote_count')

    df_etl = data_df.where((data_df.id.isNotNull()) & (data_df.budget !=0) & (data_df.genres.isNotNull()) & (data_df.production_companies !='[]') & (data_df.release_date.isNotNull()) & (data_df.revenue !=0) & (data_df.revenue.isNotNull()) & (data_df.runtime.isNotNull()))


    movies_rdd = df_etl.select('id','genres').rdd.map(list)
    #movies_rdd = df_etl.rdd.map(list)

    movies_rdd = movies_rdd.map(getValues)
    
    movies_rdd = movies_rdd.filter(lambda x : x!=None)
    #print(movies_rdd.take(10))
    movies_df = movies_rdd.toDF()

    movies_df = movies_df.withColumnRenamed('_1','id1')
    movies_df = movies_df.withColumnRenamed('_2','genres1')
    
    cond = movies_df.id1 == df_etl.id
    df_etl = movies_df.join(df_etl,cond).select('budget','genres1','id','original_language','popularity','production_companies',
        'production_countries','release_date','revenue','runtime','title','vote_average','vote_count')
    
    df_etl = df_etl.withColumnRenamed('genres1','genres')

    df_etl.show()
    df_etl.write.format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=keyspace).save()

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs,keyspace,table)