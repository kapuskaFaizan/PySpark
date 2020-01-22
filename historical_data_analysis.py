# Importing all the dependencies
import pyspark
import json
from pyspark.sql import SparkSession
import pandas as pd
import re
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import LDA


# Connection to spark cluster
sc = SparkContext() 
sqlc = SQLContext(sc)

# Data Collection / parsing
df = sqlc.read.json('file:///home/codersarts/Desktop/env-py/proj-pyspark/data/tweets.json')

# Feature engineering
feature_cols = ['lang','place','retweeted','text']
drop_cols = [col for col in df.columns if col not in feature_cols]

f_df = df.drop(*drop_cols)
f_df = f_df.dropna(subset=feature_cols)

feat_df = f_df.withColumn('text',regexp_replace('text', '((www\.[\S]+)|(https?://[\S]+))', ''))

feat_df = feat_df.withColumn('text',regexp_replace('text', '@[\S]+', ''))

new_df = feat_df.withColumn('text',regexp_replace('text', '#(\S+)', ''))

# Save features to outfile
collected_df = new_df.collect()
with open('/home/codersarts/Desktop/env-py/proj-pyspark/data/features.json', 'w') as outfile:
    json.dump(collected_df, outfile)


# Preprocessing
tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(new_df)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)


idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# Model defnition 
lda = LDA(k=7, maxIter=10)    #here change k for forming any no. of clusters
model = lda.fit(rescaledData.select('features'))
ll = model.logLikelihood(rescaledData.select('features'))
lp = model.logPerplexity(rescaledData.select('features'))

print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound on perplexity: " + str(lp))

# Describe clusters.
topics = model.describeTopics(5)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)

# Make predictions
transformed = model.transform(rescaledData.select('features'))
transformed.show(truncate=False)
print(transformed.columns)

# Result -> target field -> dense vector
result = transformed.select('topicDistribution').collect()
result_final = [ str(i) for i in result]

# Save json file 
with open('/home/codersarts/Desktop/env-py/proj-pyspark/data/RESULT.json', 'w') as outfile:
    json.dump(result_final, outfile)