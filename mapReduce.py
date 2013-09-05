
import pymongo as pm
import bson
import ConfigParser
import logging

from bson.code import Code
#from pymongo import Connection
#from pymongo.code import Code

#print('dir:pymongo:',dir(pymongo))
#print('pymongo version:', pymongo.version)


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

db = mongo_login()

try:
	#'''
	#Open a connection to MongoDb (localhost)
	#connection =  Connection()
	#db = connection.test

	

	#Remove any existing data
	#db.texts.remove()

	#Insert the data 
	lines = open('2009-Obama.txt').readlines()
	[db.texts.insert({'text': line}) for line in lines]

	#Load map and reduce functions
	map = Code(open('wordMap.js','r').read())
	reduce = Code(open('wordReduce.js','r').read())


	#Run the map-reduce query
	results = db.texts.map_reduce(map,reduce, "myresults")

	#Print the results
	for result in results.find():
		print result['_id'] , result['value']['count']

	db.logout()
except Exception as e:
	print('an exception occurred!')
	print(str(e))
	db.logout()
