import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types,Row
from cassandra.cluster import Cluster


spark = SparkSession.builder.appName('Cassandra Dataframe').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(inputs,keyspace,table):
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

    credits_schema = types.StructType([
    types.StructField('cast', types.StringType()),
    types.StructField('crew', types.StringType()),
    types.StructField('id', types.IntegerType()),
    
])

    credits_df = spark.read.csv(inputs,schema=credits_schema,header=True)
    credits_clean_df = credits_df.where((credits_df.cast.isNotNull()) & (credits_df.crew.isNotNull()) & (credits_df.id.isNotNull()))

    credits_clean_df = credits_clean_df.where((credits_clean_df.cast !='[]') & (credits_clean_df.crew !='[]'))
    
    
    credits_clean_df.write.format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=keyspace).save()

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs,keyspace,table)
