# twitter_stream_listener.py

import sys
import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


# You need to change the following information

API_KEY = "CNTE4qBEPNdummyMhDx1lReh" # Consumer Key (API Key)
API_SECRET = "9XdummyDzix4CNTE4qBEPN" # Consumer Secret (API Secret)
TOKEN_KEY =  "EcToAIaeP1dummyZ824-VjuwjkLLiL2Xb" # Access Token
TOKEN_SECRET = "irHMF7ymPUlSz1mNH8qktdummy7c" # Access Token Secret


def getTwitterOAuth():
  auth = OAuthHandler(API_KEY, API_SECRET)
  auth.set_access_token(TOKEN_KEY, TOKEN_SECRET)
  return auth


class MyListener(StreamListener):
 """A child class from Tweepy StreamListener."""
 
 def __init__(self,time_limit=30, fname="stream_test"):
  self.time = time.time() 
  self.time_limit = time_limit
  self.outfile = "%s.jsonl" % fname
  print("Downloading stream...")
  
 def on_data(self, data):
  
  f = open(self.outfile,'a')
  
  while((time.time() - self.time) < self.time_limit):
   try:
    f.write(data)
    return True
   except BaseException as e:
    print("Error: {}\n".format(e))
    time.sleep(5) # After an error we are going to wait 5 seconds
    return True
    
  f.close()
  print("Done.")
  exit()
  
   
 def on_error(self, status):
  if status == 420:
   print("Rate limit exceeded\n")
   return False
  else:
   print("Error {}\n".format(status))
   return True
    


if __name__ == '__main__':
 
 query = sys.argv[1:] # List of keywords
 auth = getTwitterOAuth()
 twitter_stream = Stream(auth, MyListener())
 twitter_stream.filter(track=query, async=True)
