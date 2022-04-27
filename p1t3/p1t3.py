from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

temp = spark.read.csv('gs://4121programminghw2/temp_small_file.csv')
temp = temp.selectExpr("_c0 as title", "_c1 as articles")
temp = temp.dropna()


from pyspark.sql.functions import lit, expr, sum, asc
df1 = temp.groupby('title').count()
df1 = df1.withColumn('rank', lit(1))
df1 = df1.selectExpr("title as title", "count as neighbors", "rank as rank")
for _ in range(10):
    df2 = temp.join(df1, on='title', how='right')
    df2 = df2.withColumn('contri', expr('rank/neighbors'))
    df3 = df2.select('articles', 'contri').groupby('articles').agg(sum('contri').alias('contri'))
    df3 = df3.withColumn('rank', expr('0.85*contri+0.15')).dropna()
    df1 = df1.select('title', 'neighbors').join(df3.selectExpr('articles as title', 'rank as rank'), 
                                            on='title', how='right').na.fill({'neighbors': 0})


df1.select("title", "rank").sort(asc("title"), asc("rank")).limit(5).write.csv('gs://4121programminghw2/pr_small.csv', sep='\t')