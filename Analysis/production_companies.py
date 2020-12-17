from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as f
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
import ast



def prod_companies(x):
    mylist = []
    for i in ast.literal_eval(x[2]):
        mylist.append(i['name'])
    mylist1 = mylist
    mylist = []    
    return [x[0],x[1],mylist1,x[3],x[4],x[5],x[6],x[7],x[8],x[9]]

#udf method to find the decade 
@functions.udf(returnType=types.StringType())
def cal_decade(year) :
    diff = int(year)%10
    if diff == 0:
        start_year = int(year)
    else:
        start_year = int(year)-diff
    end_year = start_year+10
    decade = str(start_year)+"-"+str(end_year)
    return decade 

#extracting production companies in the required form
def getcompanies(x):
    y = x[2]
    z = y[1:len(y)-1]
    return [x[0],x[1],z.split(','),x[3],x[4],x[5],x[6],x[7],x[8]]





def main(table1, keyspace, output):

	#loading the data from cassandra database into dataframe
    profits_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table=table1, keyspace=keyspace).load()


    profits_df = profits_df.select('id', 'budget', 'production_companies', 'revenue', 'title','release_date',f.split(profits_df.release_date,'-').alias('release_year'))
    profits_df = profits_df.withColumn("year_of_release",profits_df['release_year'].getItem(0))
    profits_df = profits_df.withColumn('profit',profits_df.revenue - profits_df.budget)
    profits_df = profits_df.where(profits_df.profit > 0)
    profits_df =profits_df .withColumn('profit_percentage', (profits_df.profit/profits_df.budget)*100)
    

    profits_rdd = profits_df.rdd.map(list)
    profits_rdd = profits_rdd.map(prod_companies)

    profits_rdd = profits_rdd.filter(lambda x : x!=None)
    profits_df = profits_rdd.toDF()
    
    #defining schema
    profits_df = profits_df.withColumnRenamed('_1','id')
    profits_df = profits_df.withColumnRenamed('_2','budget')
    profits_df = profits_df.withColumnRenamed('_3','production_companies')
    profits_df = profits_df.withColumnRenamed('_4','revenue')
    profits_df = profits_df.withColumnRenamed('_5','title')
    profits_df = profits_df.withColumnRenamed('_6','release_date')
    profits_df = profits_df.withColumnRenamed('_7','release_year')
    profits_df = profits_df.withColumnRenamed('_8','year_of_release')
    profits_df = profits_df.withColumnRenamed('_9','profit')
    profits_df = profits_df.withColumnRenamed('_10','profit_percentage')

    profits_df = profits_df.select('id', 'budget', 'production_companies', 'revenue', 'title','year_of_release','profit','profit_percentage')


    df_decade = profits_df.withColumn("decade",cal_decade(profits_df['year_of_release'])).select('id', 'budget', 'production_companies', 'revenue', 'title', 'year_of_release','profit','profit_percentage','decade')
    df_decade.printSchema()	
 

    df_decade = df_decade.select('id','budget',functions.explode(df_decade["production_companies"]).alias('production_companies'),'revenue','title','profit','profit_percentage','decade')
    
    df_decade = df_decade.groupBy('production_companies','decade').sum('profit')
    df_decade = df_decade.withColumnRenamed('sum(profit)', 'profit')
    df_decade = df_decade.sort(df_decade['decade'])
    df_decade.printSchema()
    #finding profitable production companies.
    df = df_decade.groupBy('decade').max('profit')
    df = df.withColumnRenamed('max(profit)', 'profit1')
    df = df.withColumnRenamed('decade','decade1')
    cond = [df_decade.profit == df.profit1, df_decade.decade == df.decade1]
    df1 = df_decade.join(df,cond)
    df2 = df1.select('decade','production_companies','profit')
    df2 = df2.sort(df1['decade'])
    df2.show(10)
    #writing into csv file.
    df2.write.csv(output,mode='overwrite')



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
    keyspace = sys.argv[2]
    output = sys.argv[3]
    main(table1 ,keyspace,output)