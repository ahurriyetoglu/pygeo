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
	index_result = db.places.create_index([("loc", GEO2D)])
	print('Indexing Result:', index_result)

	db.places.remove()
	db.places.insert({"loc": [2, 5]})
	db.places.insert({"loc": [30, 5]})
	db.places.insert({"loc": [1, 2]})
	db.places.insert({"loc": [4, 4]})
	db.places.insert({"loc": [5, 6]})
	db.places.insert({"loc": [7, 5]})
	db.places.insert({"loc": [8, 6]})

	print('$near operator Example')
	for doc in db.places.find({"loc": {"$near": [3, 6]}}).limit(4):
		print(doc)
		repr(doc)

	print('$within - $box operators Example')
	for doc in db.places.find({"loc": {"$within": {"$box": [[2, 2], [5, 6]]}}}):
		print(doc)
		repr(doc)

	print('$within - $center operators Example')
	for doc in db.places.find({"loc": {"$within": {"$center": [[0, 0], 6]}}}):
		print(doc)
		repr(doc)

	db.places.remove()
	index_result = db.places.create_index([("coordinates.coordinates", GEO2D)])
	print('Indexing Result:', index_result)
	db.places.insert({"coordinates":{"coordinates":[2,5]}})
	db.places.insert({"coordinates":{"coordinates":[5,6]}})
	db.places.insert({"coordinates":{"coordinates":[3,7]}})
	db.places.insert({"coordinates":{"coordinates":[5,8]}})
	db.places.insert({"coordinates":{"coordinates":[2,4]}})
	db.places.insert({"coordinates":{"coordinates":[8,7]}})
	db.places.insert({"coordinates":{"coordinates":[1,3]}})
	db.places.insert({"coordinates":{"coordinates":[7,6]}})


	print('Embedded $near operator Example')
	for doc in db.places.find({"coordinates.coordinates": {"$near": [3, 6]}}).limit(4):
		print(doc)
		repr(doc)

	print('Embedded $within - $box operators Example')
	for doc in db.places.find({"coordinates.coordinates": {"$within": {"$box": [[2, 2], [5, 6]]}}}):
		print(doc)
		repr(doc)

	print('Embedded $within - $center operators Example')
	for doc in db.places.find({"coordinates.coordinates": {"$within": {"$center": [[0, 0], 6]}}}):
		print(doc)
		repr(doc)







db.logout()
