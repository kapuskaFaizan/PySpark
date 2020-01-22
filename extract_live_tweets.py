from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import os, sys

#Variables that contains the user credentials to access Twitter API 
access_token = "41644104-e7ObeMxiEOAkZ3PUu7cil2PhnglgV4wLjRKFuUVML"
access_token_secret = "sQvRCQVlnQ3jbR9ddVGJnRjGrgvvXGmz5fxjLsqbyZT3u"
consumer_key = "BI6IVrrX9S3PB0lBtnju9Dq1G"
consumer_secret = "5QBDVOzRZ2IDjxtxLoyl0dPxoq2xqdB0oDcpHHM1gxus1Wb0oS"

#file name where the tweets will be collected. The file would be created in current directory where you run the jupyter notebook
fileName = "my_tweets.json"
file = open(fileName,"w+") # we open file once with parameter "w+" that means apppend mode, when the file is exist instead of re-writing it, it will add new records to the end of file
counter =0 #additional variable to count how much tweets alredy collected
numTweets = 100 #Number of tweets to collect
class StdOutListener(StreamListener):
    #event function when some data is streamed from Twitter
    def on_data(self,data):
        try:
            tweet = json.loads(data)
            #print(tweet)
            text = tweet["text"]
            date = tweet["user"]["created_at"]
            user_id = tweet["user"]["id"]
            source = tweet["source"]
            time_zone = tweet["user"]["time_zone"]
            location= tweet["user"]["location"]
            language = tweet["user"]["lang"]
            timestamp = tweet["timestamp_ms"]
            coordinates = tweet["coordinates"]
            place = tweet["place"]
            hashtags = tweet["entities"]["hashtags"]
            language_t =tweet["lang"]
            user_name=tweet["user"]["name"]
            user_descr=tweet["user"]["description"]
            followers_count=tweet["user"]["followers_count"]
            friends_count=tweet["user"]["friends_count"]
            statuses_count=tweet["user"]["statuses_count"]
            #create tweet jwon object
            jsonObject = getJson(text,date,user_id,source,time_zone,location,language,timestamp,coordinates,place,hashtags,language_t,user_name,user_descr,followers_count,friends_count,statuses_count)
            #save to file
            saveToFile(jsonObject)
        except:
            text = "NONE"
        return True 
    def on_error(self,status):
            print(status)
def saveToFile(jsonObject):    
    global file
    global counter
    global numTweets
    
    
    if counter<numTweets:
        if counter == 0:
            data = jsonObject
        else:
            data ="\n"+ str(jsonObject)
        counter = counter +1   
        file.write(data)
    else:
        file.close()
        print("DONE!") 
def getJson(text,date,user_id,source,time_zone,location,language,timestamp,coordinates,place,hashtags,language_t,user_name,user_descr,followers_count,friends_count,statuses_count):
  jsonObject = {}
  jsonObject["text"] = text
  jsonObject["date"] = date
  jsonObject["userId"]=user_id
  jsonObject["source"]=source
  jsonObject["time_zone"]=time_zone
  jsonObject["location"]=location
  jsonObject["language"]=language
  jsonObject["timestamp"]=timestamp
  jsonObject["coordinates"]=coordinates
  jsonObject["place"] = place
  jsonObject["hashtags"] = hashtags
  jsonObject["tweet_lang"] = language_t
  jsonObject["user_name"]=user_name
  jsonObject["user_descr"]=user_descr
  jsonObject["followers_count"]=followers_count
  jsonObject["friends_count"]=friends_count
  jsonObject["statuses_count"]=statuses_count
  return json.dumps(jsonObject,ensure_ascii=False)

#This handles Twitter authetification and the connection to Twitter Streaming API
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'Star Wars'
stream.filter(track=['Star Wars'])