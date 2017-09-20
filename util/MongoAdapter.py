import pprint

from pymongo import MongoClient

# =============================================
# Purpose: 	This is for python3.6
# Remark:  	http://api.mongodb.com/python/current/tutorial.html?_ga=2.143540853.1702365733.1505872156-198557749.1487407623#making-a-connection-with-mongoclient
# Install: 	(1) http://api.mongodb.com/python/current/installation.html?_ga=2.145009524.1702365733.1505872156-198557749.1487407623
#               python -m pip install pymongo
# Author: 	welson
# =============================================
class MongoAdapter:
	# creates a single class instance, sets pool size but won't connect until used (lazy)
	# options = {
	# 	max_pool_size : 10,
	# 	connect_timeout : 30.0,
	# 	heartbeat_frequency : -1
	# }
	# conn_pool = Mongo::Client.new(["127.0.0.1:27017"], options)

	def __init__(self, host, port, db_name):
		self.host = host or 'localhost'
		self.port = port or 27017
		self.db_name = db_name
		self.auto_close = True
		self.max_pool_size = 10
		
		print("host={}, port={}, db_name={}".format(self.host, self.port, self.db_name))

		# Connection Pool: http://api.mongodb.com/python/current/faq.html#how-does-connection-pooling-work-in-pymongo
		# self.client = MongoClient(self.host, self.port, self.max_pool_size)
		self.client = MongoClient(self.host, self.port)
		self.db = self.client[self.db_name]

	def use(self, db_name):
		self.db = self.client[self.db_name]

	# =============================================
	# Purpose:  find
	# Params: 	query: { key: 'value' }
	#           orderBy: { 'name' : -1 }
	# Return:
	# Remark: 	  
	# Author:   welson
	# =============================================
	def find(self, coll_name, query, orderBy=None, _skip=-1, _limit=-1):
		opts = {}
		opts['orderBy'] = orderBy
		opts['skip'] = _skip
		opts['limit'] = _limit
		return self.query(coll_name, query, opts)

	# =============================================
	# Purpose: query mongo
	# Params:  query: { 'key' : 'value' }
	#            opts = {
	#              orderBy: { 'name' : -1 },
	#              skip: 0,
	#              limit: 30,
	#              fields: ["f1", "f2"]
	#            }
	# Return:   mongo documents; [] if not found
	# Remark: 	sort: http://api.mongodb.com/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.sort		
	# Author:   welson
	# =============================================
	def query(self, coll_name, query, _opts={}):
		resp = {}
		view = None
		try:
			# {}.get(key) will not raise Exception and return None if key not avail.
			orderBy = _opts.get('orderBy') 		# json
			_skip = _opts.get('skip') or -1		# int
			_limit = _opts.get('limit')	or -1	# int
			fields = _opts.get('fields') 		# fail?

			coll = self.use_collection(coll_name)

			if fields:
				# view = coll.find(query, { 'fields': fields })
				view = coll.find(query)
			else:
				view = coll.find(query)
			
			# ordering: switch JSON to python tuple-list
			# ex: [("field1", 1), ("field2", -1)]
			if orderBy:
				sorting_list = []
				for k in orderBy.keys():
					sorting_list.append( (k, orderBy[k]) )

				view = view.sort(sorting_list)

			totalCount = view.count()

			if _skip >= 0:
				view = view.skip(_skip) 

			if _limit >= 0:
				view = view.limit(_limit) 
			
			queryResult = view
			returnCount = view.count()

			resp['totalCount'] = totalCount
			resp['returnCount'] = returnCount
			resp['queryResult'] = list(queryResult)
			return resp
		except Exception as e:
			print(e)
			raise e
		finally:
			if self.auto_close:
				self.close()
		
			
	# =============================================
	# Purpose:  Disconnect from MongoDB.
	# Params: 
	# Return:     
	# Remark: 	http://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient.disconnect
	# 			If this instance is used again it will be automatically re-opened and the threads restarted
	# Author:   welson
	# =============================================
	def close(self):
		if self.client:
			self.client.close()
			# self.client = None

	def use_collection(self, coll_name):
		if self.client == None:
			self.client = MongoClient(self.host, self.port)
			
		self.db = self.client[self.db_name]

		return self.db[coll_name]
