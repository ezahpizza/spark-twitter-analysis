# Standard Library
import re
from json import loads

# Third-Party Libraries
from kafka import KafkaConsumer
from nltk import * 
from pymongo import MongoClient

# PySpark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
db = client['bigdata_project'] 
collection = db['tweets'] 

# Download stopwords
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

# SparkSession creation
spark = SparkSession.builder \
    .appName("classify tweets") \
    .getOrCreate()

# Load the model
pipeline = PipelineModel.load("logistic_regression_model.pkl")

# Clean the text
def clean_text(text):
    if text is not None:
        text = re.sub(r'https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+', '', text)
        text = re.sub(r'(@|#)\w+', '', text)
        text = text.lower()
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    else:
        return ''
    
class_index_mapping = { 0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant" }

# Kafka Consumer
consumer = KafkaConsumer(
    'numtest',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    tweet = message.value[-1]  
    preprocessed_tweet = clean_text(tweet)
    data = [(preprocessed_tweet,),]  
    data = spark.createDataFrame(data, ["Text"])
    processed_validation = pipeline.transform(data)
    prediction = processed_validation.collect()[0][6]

    print("-> Tweet:", tweet)
    print("-> preprocessed_tweet : ", preprocessed_tweet)
    print("-> Predicted Sentiment:", prediction)
    print("-> Predicted Sentiment classname:", class_index_mapping[int(prediction)])
    
    tweet_doc = {
        "tweet": tweet,
        "prediction": class_index_mapping[int(prediction)]
    }

    collection.insert_one(tweet_doc)

    print("/"*50)