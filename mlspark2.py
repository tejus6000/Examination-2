# Databricks notebook source
# importing pyspark
import re
import json
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# COMMAND ----------

# starting spark session
spark = SparkSession.builder.appName('exam2').getOrCreate()
sparkContext=spark.sparkContext

# COMMAND ----------

df = spark.read.table("default.student_data")

# COMMAND ----------

numeric_features = [t[0] for t in df.dtypes if t[1] == 'bigint' or t[1] == 'double']

# COMMAND ----------

dff = df.select('AtRisk_Academic','major','gender','c01','c02','c03','c04','c05','c06','c07','c08','c09','c10','academic','campus','internship')
cols = dff.columns

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

stages = []
categoricalColumns = [
    'major','gender'
]

for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoder(
        inputCols=[stringIndexer.getOutputCol()], 
        outputCols=[categoricalCol + "classVec"]
    )
    stages += [stringIndexer, encoder]
    

numericCols = ['c01','c02','c03','c04','c05','c06','c07','c08','c09','c10','academic','campus','internship' ]
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]   

# COMMAND ----------

from pyspark.ml import Pipeline

pipeline = Pipeline(stages = stages)
pipelineModel = pipeline.fit(dff)
dff = pipelineModel.transform(dff)
selectedCols = ['features'] + cols
dff = dff.select(selectedCols)
dff.printSchema()

# COMMAND ----------

# splitting the data as train and test sets
final_data = dff.select('features','AtRisk_Academic')
train_data,test_data=final_data.randomSplit([0.75,0.25])
train_data.describe().show()

# COMMAND ----------

# importing the ml libraries
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
import pickle

# COMMAND ----------

lr = LogisticRegression(featuresCol = 'features', labelCol  = 'AtRisk_Academic')
model = lr.fit(train_data)
fullpredictions = model.transform(test_data)
fullpredictions.select("prediction","AtRisk_academic","features").show(10)

# COMMAND ----------

try:
    model.save("/databricks/driver/myLR.sav")
except:
    print("model exsists")

# COMMAND ----------

#!pip install confluent_kafka
from confluent_kafka import Consumer
import time

# COMMAND ----------

conf = {
        'bootstrap.servers' : 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092',
        'security.protocol' : 'SASL_SSL',
        'sasl.mechanisms':'PLAIN',
        'sasl.username':'K5O4CLAR3YZOFJID',
        'sasl.password' : '2wEeF5wsKTvyIqnmtcusN5vrjgMjghBvUp8WThNGinuv5txYBwOTZoFzhOF6a+3P',
        'group.id' : 'id-2'
        }        
print("connecting to Kafka topic")

# COMMAND ----------

consumer = Consumer(conf)
#consumer = Consumer(conf2)
consumer.subscribe(['customerOrders'])
#consumer.subscribe(['topic_2'])

t_end = time.time() + 60 * 2
list_json=[]

while time.time() < t_end:
    msg = consumer.poll(1.0)
    #msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error happened: {}".format(msg.error()))
        continue
    #print("Connected to Topic: {} and Partition : {}".format(msg.topic(), msg.partition() ))
    #print("Received Message : {} with Offset : {}".format(msg.value().decode('utf-8'), msg.offset() ))
    # Replace ' to " 
    temp_msg=msg.value().decode('utf-8').replace('\'','"')
    #Change to dict
    temp_msg=json.loads(temp_msg)
    list_json.append(temp_msg)
    for item in list_json:
        for key, value in item.items():
            try:
                item[key] = float(value)
            except ValueError:
                continue
    if len(list_json)==10:
        #final_schema = schema
        m=spark.createDataFrame(list_json, schema=None)
        m = m.select('AtRisk_Academic','major','gender','c01','c02','c03','c04','c05','c06','c07','c08','c09','c10','academic','campus','internship')
        cols = m.columns
        m=pipelineModel.transform(m)
        model = LogisticRegressionModel.load("/databricks/driver/myLR.sav")
        model_predictions = model.transform(m)
        model_predictions.select("prediction","AtRisk_academic","features").show(10)
        list_json=[]
print(len(list_json))

time.sleep(2.5)

# COMMAND ----------

#!pip install pymongo
import pymongo
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient

# COMMAND ----------

client = pymongo.MongoClient("mongodb+srv://pain:pain@serverlessinstance0.8n6hg.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
db = client['student_db']
finalC = db['student_pred']

# COMMAND ----------

finalC.count_documents({})

# COMMAND ----------

type(model_predictions)

# COMMAND ----------

outfile = model_predictions.select("prediction","AtRisk_Academic").toPandas()

# COMMAND ----------

outfile.to_csv('outfile.csv')

# COMMAND ----------

outfileDict = outfile.to_dict('records')
outfileDict[:]

# COMMAND ----------

finalC.insert_many(outfileDict)
