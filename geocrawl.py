# -*- coding: utf-8 -*-

#python 2.7# -*- coding: utf-8 -*-
import os
import re
import operator
import datetime
import calendar
import logging
import time
import threading
import urllib, urllib2
import json
import ConfigParser
import tweepy
import pymongo as pm
from ssl import SSLError


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime

import Queue



class StdOutListener(StreamListener):
	'''

	Notes:
	1- define.db..., strptime*()

	'''

	def settings( self, dbnp = None, queue= None ):
	    self.errorCount = 0 # trial!
	    self.queue = queue
	    self.tweet_count  = 0
	    self.geononecount = 0
	    self.db = dbnp


	def on_data(self, data): # from Manos
		try:
			tweeto = json.loads(data)
			print('Not Parsed tweet:',data) # data is string type
			print('Tweet parsed.')
		except Exception:
			print("Failed to parse tweet data..")
			tweeto = None

		print('Dir of Data:', type(data),dir(data))
		# print('Geo of Data:', data.geo)

		if tweeto:
			if tweeto.has_key('id') and tweeto.has_key("text") and tweeto.has_key("coordinates"):
				print('Parsed Tweet:',tweeto)
				# print(dir(tweeto), tweeto.keys())
				print("%s: %s" % (tweeto['user']['screen_name'].encode('UTF-8'), tweeto['text'].encode('UTF-8')))
				if tweeto['coordinates'] != None:
					ins_id = self.db.insert(tweeto)
					print('Inserted with ID:', ins_id)
					print ('Tweet count:', self.tweet_count, tweeto['coordinates'], type(tweeto['coordinates']), dir(tweeto['coordinates']),tweeto['coordinates']['type'], tweeto['coordinates']['coordinates'])

				else:
					print('coordinates is None')



				tweeto['doc_type'] = "tweet"

				self.tweet_count += 1

		else:
			print("Received a response which is not a tweet or without Geo!")
			print(data)

		# if 'in_reply_to_status_id' in data:
		# 	status = Status.parse(self.api, tweeto)
		# 	if self.on_status(status) is False:
		# 		return False
		# elif 'delete' in data:
		# 	delete = tweeto['delete']['status']
		# 	if self.on_delete(delete['id'], delete['user_id']) is False:
		# 		return False
		# elif 'limit' in data:
		# 	if self.on_limit(tweeto['limit']['track']) is False:
		# 		return False

		# # Finally, store the tweet object in the archive. The record
		# # will be updated with song and artist as soon the processing
		# # pipeline finishes.
		# self.db.archive.update({'tweetid': tweeto["id"]}, {"$set": {'tweet':tweeto}}, upsert=True)


	def on_data_1(self, data): #copied from:http://pmatigakis.wordpress.com/2012/12/01/twitter-data-mining-crawling-twitter/
		try:
			tweet = json.loads(data)
		except Exception:
			print("Failed to parse tweet data")
			tweet = None

		if tweet:
			if tweet.has_key('id') and tweet.has_key("text"):
				print("%s: %s" % (tweet['user']['screen_name'], tweet['text']))

				tweet['doc_type'] = "tweet"

				self.db["tweet:%d" % tweet['id']] = tweet

				self.tweet_count += 1
			elif not self.received_friend_ids and tweet.has_key("friends"):
				print("Got %d user ids" % len(tweet['friends']))
				self.received_friend_ids = True
		else:
			print("Received a responce that is not a tweet")
			print tweet

		return True

	def on_status_2(self, status): # from https://github.com/Ccantey/GeoSearch-Tweepy/blob/master/GeoTweepy.py
		#print "Tweet Text: ",status.text
		text = status.text
		#print "Time Stamp: ",status.created_at
		try:
			Coords.update(status.coordinates)
			XY = (Coords.get('coordinates'))  #Place the coordinates values into a list 'XY'
			#print "X: ", XY[0]
			#print "Y: ", XY[1]
		except:
			#Often times users opt into 'place' which is neighborhood size polygon
			#Calculate center of polygon
			Place.update(status.place)
			PlaceCoords.update(Place['bounding_box'])
			Box = PlaceCoords['coordinates'][0]
			XY = [(Box[0][0] + Box[2][0])/2, (Box[0][1] + Box[2][1])/2]
			#print "X: ", XY[0]
			#print "Y: ", XY[1] 
			pass
			# Comment out next 4 lines to avoid MySQLdb to simply read stream at console
		curr.execute("""INSERT INTO TwitterFeed2 (UserID, Date, X, Y, Text) VALUES (%s, %s, %s, %s, %s);""", (status.id_str,status.created_at,XY[0],XY[1],text))
		
		db.commit()




	def on_status( self, status):
		#conv_datetime = datetime.strptime(querytime, "%a %b %d %H:%M:%S +0000 %Y")

		#print "Type of status:'%s createdAt:"%(type(status))
		self.tweetcount += 1 # just a track to see
		
		if status.geo != None:
			print "screen_name='%s' tweet='%s' createdAt='%s' "%(status.author.screen_name, status.text, status.created_at)
			print 'Tweet count:', self.tweet_count, status.geo
			#print dir(status)
		else:
			pass
			# self.geononecount += 1
			# print 'None Count:', self.geononecount

		#print self.tweetcount
		#print dir(status)

	def on_error( self, status ):
		logging.debug("Error returned by twitter: %s" % str(status) )
		self.errorCount += 1

		if self.errorCount > 1000: return False
		return
	    
	    
	def on_timeout( self ):
		# If no post received for too long
		logging.debug("Stream exiting at %s" % str( datetime.datetime.now()) )
		sys.exit()
		return
	    
	    
	def on_limit( self, track ):
		# If too many posts match our filter criteria and only a subset is sent
		logging.debug("Limit notification returned by twitter: %s" % str(track) )
		return
	    
	    
	def on_delete( self, status_id, user_id ):
		# When a delete notice arrives for a post.
		return

def twitter_login():
	'''
	OAuth format:
	----start: dots should be filled by keys taken from Twitter:https://dev.twitter.com/apps/new
	[twitterapp]
	consumer_key = ..
	consumer_secret = ..

	access_token = ..
	access_token_secret = ..

	---end:


	'''
	config = ConfigParser.ConfigParser()
	config.read('oauth.ini')


	consumer_key = config.get('twitterapp', 'consumer_key')
	consumer_secret = config.get('twitterapp', 'consumer_secret')

	access_token = config.get('twitterapp', 'access_token')
	access_token_secret = config.get('twitterapp', 'access_token_secret')

	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	return auth

def mongo_login():
	'''
	OAuth format:
	----start: dots should be filled by keys taken from Twitter:https://dev.twitter.com/apps/new
	[mongodb]
	

	---end:


	'''
	config = ConfigParser.ConfigParser()
	config.read('oauth.ini')

	servername = config.get('mongodb','servername')
	port = config.get('mongodb', 'port')
	port = int(port)
	username = config.get('mongodb', 'username')
	passw = config.get('mongodb','passw')

	""" Connect to MongoDB and set up database """
	logging.info("Connecting to MongoDB")

	# print('dir_mongoclient', dir(mclient))
	mclient = pm.MongoClient(servername, port)

	dbnp = mclient.geotwitter
	dbnp.authenticate(username, passw)

	logging.info("...connected")

	
	return dbnp 



if __name__ == "__main__":

	
	try:
		auth = twitter_login()
		api = tweepy.API(auth)
		print api.me().name.encode('UTF-8')
		l = StdOutListener()

		streaming_api = tweepy.streaming.Stream(auth,l, timeout=60)


		dbnp = mongo_login()
		print(dbnp.collection_names())

		print('\n---------------',dbnp, dir(dbnp))#help(dbnp),'\n----------------------')

		gtweets = dbnp.geotweets

		l.settings(gtweets)

		#gtweets.find(one)
		
		# filter uses OR for between track and locations.
		#streaming_api.filter(track= ['bir', 'ben']) #, locations=[-180,-90,180,90] ) # any geotagged tweet:-180,-90,180,90 ,40.25,28.2 # locations=[-122.75,36.8,-121.75,37.8]
		                                                                                         # locations=[28.75,40,29.75,41], [41, 28, 42, 29]
		                                                            # track = ['gezi', 'diren']
		streaming_api.filter(locations=[-180,-90,180,90])
	except KeyboardInterrupt:
		streaming_api.disconnect()
		dbnp.logout()
		print "got keyboardinterrupt"


# if __name__ == "__main__":

# 	queue = Queue.Queue()

# 	l = StdOutListener()
#     l.settings(queue)

	

# 	while True:
#         try:
#             stream.filter(track=['#np','#nowplaying'])
#         except Exception:
#             logging.exception('stream filter')
#             time.sleep(10)
#         except SSLError:
#             logging.exception('SSL exception')
#             time.sleep(2)

# 	api = tweepy.API(auth)

# 	print api.me().name

# If the application settings are set for "Read and Write" then
# this line should tweet out the message to your account's 
# timeline. The "Read and Write" setting is on https://dev.twitter.com/apps
#api.update_status('Updating using OAuth authentication via Tweepy!')