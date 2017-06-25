# AEMET Opendata parser
# Spain meteo Agecy AEMET has started an OpenData project...
# Before I was using the CSV files available, but, well, clumsy stuff

import httplib, urllib, ssl, json, requests, time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Load the API key from file api.key
# Please note the API key has to be obtained at https://opendata.aemet.es/centrodedescargas/altaUsuario

ssl._create_default_https_context = ssl._create_unverified_context

with open ("api.key", "r") as conffile:
    conf=conffile.readlines()

apikey=conf[0]

allLast24 = "/opendata/api/observacion/convencional/todas"

header = 'api_key: ' + apikey



uri = allLast24 + "?api_key=" + apikey

#print uri

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET",uri)
response = conn.getresponse()
data = response.read()
print data
predata = json.loads(data)

uriDatos = predata.get("datos")
uriMetadatos = predata.get("metadatos")

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET", uriDatos)
response = conn.getresponse()
data = response.read()
data = data.decode('iso-8859-1').encode('utf-8')
p = json.loads(data, 'utf-8')

num = 0

for i in p:
    num = num + 1
    print num, i['idema'],
    #print i
    # calcutating ElasticSearch ID
    rawId = i['fint'].encode('ascii','ignore') + i['idema'].encode('ascii','ignore')
    esId = filter(str.isalnum, rawId)
    print esId
    #for a, b in i.iteritems():
    #   c = str(a) + ": " + str(b) + " -"
    #    print c,
    #print
    # print "--------------------------------------------------"


