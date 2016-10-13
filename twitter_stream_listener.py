# twitter_stream_to_mongodb.py

import json
import sys
import time
from pymongo import MongoClient
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
	
	def __init__(self, coleccion, time_limit=30, fname="stream_test"):
		self.coleccion = coleccion
		self.time = time.time() 
		self.time_limit = time_limit
		self.numero_tweets = 0
		print("Hemos conectado con Twitter...")
		print("Descargando stream...")
		
	def on_data(self, data):
		
		while((time.time() - self.time) < self.time_limit):
			try:
				documento = json.loads(data) # Transformamos el json recibido en un diccionario
				self.coleccion.insert(documento) # Lo insertamos en la colección
				self.numero_tweets += 1
				return True
			except BaseException as e:
				print("Error: {}\n".format(e))
				time.sleep(5)
				return True
				
		print("Hecho. Recibidos un total de %s tweets" % self.numero_tweets)
		exit()
		
			
	def on_error(self, status):
		if status == 420:
			print("Rate limit exceeded\n")
			return False
		else:
			print("Error {}\n".format(status))
			return True
				


if __name__ == '__main__':
	
	query = 'Bob Dylan' 
	
	try:
		cliente = MongoClient()
		db = cliente['twitter_resultados']
		coleccion = db.tweets
		print("Hemos conectado con la base de datos...")
	except:
		print("No hemos podido conectar con la base de datos.")
		sys.exit()
	
	auth = getTwitterOAuth()
	try:
		twitter_stream = Stream(auth, MyListener(coleccion))
		twitter_stream.filter(track=query, async=True)
	except:
		print("No hemos podido conectar con Twitter.")
