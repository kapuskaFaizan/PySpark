# Importing dependencies

import pyspark
import json
from pyspark.sql import SparkSession
import re
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer 

# Represents a connection to spark cluster
sc = SparkContext() 
sqlc = SQLContext(sc)

# Getting streaming data from local dir.
df = sqlc.read.json('file:///home/codersarts/Desktop/env-py/proj-pyspark/data/stream_data.json')
print(df.columns)

# Feature engineering
feature_cols = ['place','text']
drop_cols = [col for col in df.columns if col not in feature_cols]

f_df = df.drop(*drop_cols)

feat_df = f_df.withColumn('text',regexp_replace('text', '((www\.[\S]+)|(https?://[\S]+))', ''))

feat_df = feat_df.withColumn('text',regexp_replace('text', '@[\S]+', ''))

new_df = feat_df.withColumn('text',regexp_replace('text', '#(\S+)', ''))

# Tokenize the document text
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
tokenized = tokenizer.transform(new_df).cache()

# Hashing and setting feature count
hashingTF = HashingTF (inputCol="tokens", outputCol="rawFeatures", numFeatures=2000)
tfVectors = hashingTF.transform(tokenized).cache()    

# Conversation into term frequency vector matrix
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=5)
idfModel = idf.fit(tfVectors)
tfIdfVectors = idfModel.transform(tfVectors).cache()

# Model initialization
lda = LDA(k=5, maxIter=10)    #here change k for forming any no. of clusters
model = lda.fit(tfIdfVectors.select('features'))
ll = model.logLikelihood(tfIdfVectors.select('features'))
lp = model.logPerplexity(tfIdfVectors.select('features'))

# printing result parameters to the screen
print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound on perplexity: " + str(lp))

# Describe clusters.
topics = model.describeTopics(5)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)

# Make predictions
transformed = model.transform(tfIdfVectors.select('features'))
transformed.show(truncate=False)
print(transformed.columns)

# Result is dense vector
result = transformed.select('topicDistribution').collect()

# Conversation of the dense vector to string for outfile
result_final = [ str(i) for i in result]

# Saving the data to outfile
with open('/home/codersarts/Desktop/env-py/proj-pyspark/data/streaming_RESULT.json', 'w') as outfile:
    json.dump(result_final, outfile)