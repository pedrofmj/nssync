import os;
import simplejson as json;
import uuid;
import time;

from datetime import datetime;
from datetime import date;

from elasticsearch import Elasticsearch;
from elasticsearch.exceptions import NotFoundError;
from cassandra.cluster import Cluster;

def datetimeFromIsoFormat(strIsoFormat):
	year = int(strIsoFormat[0:4]);
	month = int(strIsoFormat[5:7]);
	day = int(strIsoFormat[8:10]);
	hour = int(strIsoFormat[11:13]);
	minute = int(strIsoFormat[14:16]);
	second = int(strIsoFormat[17:19]);
	microsecond = int(strIsoFormat[20:]);
	return datetime(year,month,day,hour,minute,second,microsecond);

class NsSynchronizer:
	
	defaultTimestampField = '';
	count = 0;
	timeline = datetime(1900,1,1,0,0,0);
	es = Elasticsearch();
	cluster = Cluster();
	encoder = json.JSONEncoder();
	delay = 10;
	
	# Constructor
	def __init__(self,timestampField = 'timestamp'):
		self.defaultTimestampField = timestampField;
		print "Synchronizer Created";
	
	# Runs continuously always synchronizing
	def keepsynchronizing(self):
		while (1 == 1):
			self.sync();
			time.sleep(self.delay)
			self.timeline = datetime.now();
			self.timeline = datetime(self.timeline.year,self.timeline.month,self.timeline.day,self.timeline.hour,self.timeline.minute,self.timeline.second);
			
	
	# Synchronize ElasticSearch and Cassandra
	def sync(self):
		print "Synchronizer Started";
		self.dothesync();
		print "Synchronizer Finished";
		
	# TODO Dummy - Remove	
	def dothesync2(self):
		searchResultES = self.fetchES();	
		print json.dumps(searchResultES, sort_keys = False, indent = 4)
		searchResultCS = self.fetchCS();
		for user in searchResultCS:
			print user.id,user.username
	
	# TODO Dummy - Remove
	def fetchES(self):
		es = elf.es;
		# Define the timestamp field.
		timestampField = self.defaultTimestampField;
		args = locals();
		baseDate = datetime(1900,1,1,0,0,0);
		searchBodyES = '''
		{
			"query": {
				"range": {
					"%s": {
						"gte": "%s"
					}
				}
			}
		}
		''' % (timestampField,self.timeline.isoformat());
		searchResultES = es.search(body = searchBodyES);
		return searchResultES;
	
	# Index an object in ElasticSearch from a Cassandra Row
	def ESindexCSRow(self,indexName,type,row):
		rowdic = {};
		for (field,value) in row.__dict__.items():	
			if field == 'id':
				continue;
			else:				
				if isinstance(value,datetime):
					rowdic[field] = value.isoformat();
				else:
					rowdic[field] = value;
				print "\t\t\t\t\t\t%s => %s" % (field,value);
		jsonObject = self.encoder.encode(rowdic);
		self.es.index(indexName, type, jsonObject, id=row.id, params=None)

	# Update Cassandra from an ElasticSearch Object
	# Unfortunatelly, due to Cassandra search restrictions on non-key values,
	# it's needed to perform a DELETE and and INSERT instead of an UPDATE
	def CSupdatexESobject(self,session,indexName,type,row,objInES):
		source = objInES['_source'];
		newrow = {u'id': objInES['_id']};
		for field in source:
			value = source[field];
			newrow[field] = value;
		print '\t\t\t\t\t\t\t',newrow;
		query = "DELETE FROM %s.%s WHERE id = %s" % (indexName,type,objInES['_id']);
		session.execute(query);
		query = "INSERT INTO %s.%s (%s) VALUES (%s)";
		columnsPart = "";
		valuesPart = "";
		valuesList = [];
		for field in newrow:
			value = newrow[field];
			if columnsPart != "":
				columnsPart = columnsPart + ","
				valuesPart = valuesPart + ","
			columnsPart = columnsPart + field;
			if field == 'id':
				valuesPart = valuesPart + "%s";
			elif isinstance(value,(int,long)):
				valuesPart = valuesPart + "%d";
			elif isinstance(value,(float)):
				valuesPart = valuesPart + "%f";
			elif isinstance(value,(datetime,date)):
				valuesPart = valuesPart + "'%s'";
			elif isinstance(value,(str,unicode)):
				valuesPart = valuesPart + "'%s'";
			else:
				valuesPart = valuesPart + "%s";
			valuesList.append(value);
		query = query % (indexName,type,columnsPart,valuesPart);
		query = query % tuple(valuesList);
		session.execute(query);
			
	# Update Cassandra from an ElasticSearch Object
	# Unfortunatelly, due to Cassandra search restrictions on non-key values,
	# it's needed to perform a DELETE and and INSERT instead of an UPDATE
	def CSinsertESobject(self,session,indexName,type,objInES):
		source = objInES['_source'];
		newrow = {u'id': objInES['_id']};
		for field in source:
			value = source[field];
			newrow[field] = value;
			print '\t\t\t\t\t\t\t',newrow;
			query = "INSERT INTO %s.%s (%s) VALUES (%s)";
			columnsPart = "";
			valuesPart = "";
			valuesList = [];
			for field in newrow:
				value = newrow[field];
				if columnsPart != "":
					columnsPart = columnsPart + ","
					valuesPart = valuesPart + ","
				columnsPart = columnsPart + field;
				if field == 'id':
					valuesPart = valuesPart + "%s";
				elif isinstance(value,(int,long)):
					valuesPart = valuesPart + "%d";
				elif isinstance(value,(float)):
					valuesPart = valuesPart + "%f";
				elif isinstance(value,(datetime,date)):
					valuesPart = valuesPart + "'%s'";
				elif isinstance(value,(str,unicode)):
					valuesPart = valuesPart + "'%s'";
				else:
					valuesPart = valuesPart + "%s";
				valuesList.append(value);
			query = query % (indexName,type,columnsPart,valuesPart);
			query = query % tuple(valuesList);
			session.execute(query);

	# TODO This method doesn't work, once there are cassandra limitations on updating primary keys
	def CSupdateESobject(self,session,indexName,type,objInES):
		source = objInES['_source'];
		fieldsStr = "";
		valuesStr = "";
		valuesList = [indexName,type];
		dic = {};
		for field in source:
			value = source[field];
			if (field == "id"):
				continue;
				
			dic[field] = value;
			if fieldsStr == "":
				if isinstance(value, (int, long)):
					fieldsStr = "%s = %s" % (field, '%d');
				elif isinstance(value, (float)):
					fieldsStr = "%s = %s" % (field, '%f');
				else:
					fieldsStr = "%s = '%s'" % (field, '%s');
			else:
				if isinstance(value, (int, long)):
					fieldsStr = fieldsStr + ", %s = %s" % (field, '%d');
				elif isinstance(value, (float)):
					fieldsStr = fieldsStr + ", %s = %s" % (field, '%f');
				else:
					fieldsStr = fieldsStr + ", %s = '%s'" % (field, '%s');
			valuesList.append(value);
		valuesList.append(objInES['_id']);
		updateStr = "UPDATE %s.%s SET ";
		whereStr = " WHERE id = '%s'";
		clause = updateStr + fieldsStr + whereStr;
		tupleobj = tuple(valuesList);
		query = (clause % tupleobj);
		print query;

	# Synchronize a type
	def syncType(self,session,indexName,type):
		
		# Search for information on Cassandra and compare with those in ElasticSearch
		print "\t\t\tSynchronizing type %s" % (type)
		print "\t\t\tLooping over Cassandra data"
		timestampField = self.defaultTimestampField;
		baseDateTime = self.timeline;
		query = "SELECT * FROM %s WHERE %s > '%s' ALLOW FILTERING" % (type,timestampField,baseDateTime.isoformat());
		print "==> ",query;
		rows = session.execute(query);
		for row in rows:
			print "\t\t\t\t",row;
			try:
				objInES = self.es.get(indexName,row.id,type)
				print "\t\t\t\t\t",objInES;
				timestampInES = objInES['_source']['timestamp'];
				timestampInCS = row.timestamp.isoformat();
				if timestampInES > timestampInCS:
					print "\t\t\t\t\t\tElasticSearch has newer information. Updating Cassandra..."
					self.CSupdatexESobject(session,indexName,type,row,objInES);
				elif timestampInES < timestampInCS:
					print "\t\t\t\t\t\tCassandra has newer information. Updating ElasticSearch..."
					self.ESindexCSRow(indexName,type,row);
				else:
					print "\t\t\t\t\t\tInformation has same timestamp. Skipping..."
			except NotFoundError:
				print "\t\t\t\t\tNot in ElasticSearch. Inserting..."
				self.ESindexCSRow(indexName,type,row);
				
		# TODO Search for information on ElasticSearch that isn't on Cassandra. Insert on Cassandra
		print "\t\t\tLooping over ElasticSearch data"
		# Define the timestamp field.
		timestampField = self.defaultTimestampField;
		searchBodyES = '''
		{
			"query": {
				"range": {
					"%s": {
						"gte": "%s"
					}
				}
			}
		}
		''' % (timestampField,self.timeline.isoformat());
		searchResultES = self.es.search(indexName, type, searchBodyES);
		hits = searchResultES['hits']['hits'];
		#print json.dumps(hits, sort_keys = False, indent = 4);
		for hit in hits:
			print hit;
		query = "SELECT * FROM %s.%s WHERE id = %s" % (indexName,type,hit['_id']);
		rows = session.execute(query);
		size = rows.__len__();
		if size == 0:
			self.CSinsertESobject(session,indexName,type,hit)
		
		# TODO What to do with new indexes/types?
	
	# Synchronize an index		
	def syncIndex(self,systemSession,indexName):
		print "\tSynchronizing Index %s" % (indexName)
		cluster = self.cluster;
		print "\t\tFetching types";
		query = "SELECT * FROM SCHEMA_COLUMNFAMILIES WHERE KEYSPACE_NAME = '%s'" % (indexName)
		types = systemSession.execute(query);
		for type in types:
			session = cluster.connect(indexName);
			self.syncType(session,indexName,type.columnfamily_name);
	
	# Actually Perform the Synchronization
	def dothesync(self):
		cluster = self.cluster;
		systemSession = cluster.connect('system');
		print "Fetching indexes to Synchronize"
		query = "SELECT * FROM SCHEMA_KEYSPACES";
		keyspaces = systemSession.execute(query);
		for keyspace in keyspaces:
			if (keyspace.keyspace_name in ('system','system_traces')):
				continue;
			print "Synchronizing", keyspace.keyspace_name;
			self.syncIndex(systemSession,keyspace.keyspace_name);
		return;
		
	# TODO Dummy Remove
	def test(self):
		es = Elasticsearch(); # TODO: Use specified server
		
		# datetimes will be serialized
		es.index(index="my-index", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})
		#{u'_id': u'42', u'_index': u'my-index', u'_type': u'test-type', u'_version': 1, u'ok': True}
		
		# but not deserialized
		result = es.get(index="my-index", doc_type="test-type", id=42)['_source']
		#{u'any': u'data', u'timestamp': u'2013-05-12T19:45:31.804229'}
		
		print(result)
		
s = NsSynchronizer();
#s.sync();
s.keepsynchronizing();
