import pymongo as pm
import ConfigParser
import logging

from pymongo import MongoClient, GEO2D

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
	index_result = db.geotweets.ensure_index([("coordinates.coordinates", GEO2D)])
	print('Indexing Result:', index_result)


	print('Embedded $near operator Example')
	for doc in db.geotweets.find({"coordinates.coordinates": {"$near": [3, 6]}}).limit(4):
		print(doc)
		repr(doc)

	print('Embedded $within - $box operators Example')
	for doc in db.geotweets.find({"coordinates.coordinates": {"$within": {"$box": [[2, 4], [9, 15]]}}}):
		print(doc)
		repr(doc)

	print('Embedded $within - $center operators Example')
	for doc in db.geotweets.find({"coordinates.coordinates": {"$within": {"$center": [[23, 25], 5]}}}):
		print(doc)
		repr(doc)







db.logout()
