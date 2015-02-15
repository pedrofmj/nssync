import os;
import simplejson as json;

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
	
print datetimeFromIsoFormat('2015-02-14T00:36:43.288000');
