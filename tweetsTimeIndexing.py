import pymongo as pm
import ConfigParser
import datetime
import logging

from pymongo import MongoClient, GEO2D
from pymongo import ASCENDING, DESCENDING

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
	try:
		mclient = pm.MongoClient(servername, port)

		dbnp = mclient.geotwitter
		dbnp.authenticate(username, passw)

		logging.info("...connected")
	except pm.errors.ConnectionFailure, e:
		print "Could not connect to MongoDB:" #" %s" % e 

	
	return dbnp

if __name__ == "__main__":
	db = mongo_login()

	startdatetime = datetime.datetime(2013,9,6)
	enddatetime = datetime.datetime(2013,9,5,15,10)

	print ("\n NOT-Indexed:")
	print (db.geotweets.find({"created_at": {"$gt": startdatetime}}).explain())
	
	
	index_result = db.geotweets.create_index([("coordinates.coordinates", GEO2D),("created_at", 1)])
	print ("Index_Result:", index_result)
	print ("\n Indexed:")
	print (db.geotweets.find({"created_at": {"$gt": startdatetime}, "coordinates.coordinates":{"$within": {"$box": [[2, 4], [9, 15]]}).explain())

db.logout()
