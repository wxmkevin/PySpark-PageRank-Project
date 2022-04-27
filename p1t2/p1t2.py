#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

import regex
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, col, explode, lower, asc
from pyspark.sql import Row
def select_articles(text):
    if text is None:
        return []
    matches = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', text.lower())
    ans = []
    for s in matches:
        strings = s.split('|')
        for temp in strings:
            if not (not temp.startswith('category:') and ':' in temp):
                if '#' not in temp:
                    ans.append(temp)
                    break
    return ans

select_articles_udf = udf(lambda name: select_articles(name), ArrayType(StringType()))
temp = df.withColumn('title', lower(col('title'))).withColumn("articles", select_articles_udf(col("revision.text._VALUE")))
temp = temp.select(col("title"), col("articles")).withColumn('articles', explode(temp.articles))
temp.sort(asc("title"), asc("articles")).coalesce(1).limit(5).write.csv('gs://as-systems-hw2/p1t2.csv', sep='\t')


