# coding=UTF-8

# AEMET Opendata parser
# Spain meteo Agecy AEMET has started an OpenData project...
# Before I was using the CSV files available, but, well, clumsy stuff

# There is no full list of the Station IDs by province, so we build one from data
# available in the dropdowns on https://opendata.aemet.es/centrodedescargas/productosAEMET?
# Provinces are in https://opendata.aemet.es/centrodedescargas/xml/provincias.xml
# Then for each province there is a file https://opendata.aemet.es/centrodedescargas/xml/udat/udat[ID].xml

import httplib, urllib, ssl, json, requests, time
import sys
from elasticsearch import Elasticsearch
import xml.etree.ElementTree

# define our Elasticsearch host. Hardcoded, TO DO: Put into config file

es = Elasticsearch(['localhost'],
    port=9200,
    use_ssl=False)

reload(sys)
sys.setdefaultencoding('utf-8')

# Load the API key from file api.key
# Please note the API key has to be obtained at https://opendata.aemet.es/centrodedescargas/altaUsuario
# DO NOT USE my API Key, please, get your own, thanks

ssl._create_default_https_context = ssl._create_unverified_context

with open ("api.key", "r") as conffile:
    conf=conffile.readlines()

apikey=conf[0]

# Go get the .xml files to create the dictionary with all stations and their provinces

ssl._create_default_https_context = ssl._create_unverified_context

provinciasUri = '/centrodedescargas/xml/provincias.xml'

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET",provinciasUri)
response = conn.getresponse()
data = response.read()

# Creating list for the provinces
ssl._create_default_https_context = ssl._create_unverified_context

provinciasUri = '/centrodedescargas/xml/provincias.xml'

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET",provinciasUri)
response = conn.getresponse()
data = response.read()

# Creating list for the provinces
provincias = xml.etree.ElementTree.fromstring(data)
ids = []
provs = []
iter = provincias.getiterator('ID')
for elem in iter:
    elementName = elem.tag
    elementText = elem.text
    # print elem.tag, elem.text
    ids.append(elementText)

iter = provincias.getiterator('NOMBRE')
for elem in iter:
    elementName = elem.tag
    elementText = elem.text
    # print elem.tag, elem.text
    provs.append(elementText)

provdict = dict(zip(ids,provs))

a = 0

idemas = []
thisProv = []

for key in provdict:
    # we build the URL as above
    stationsUri = '/centrodedescargas/xml/udat/udat' + key + '.xml'
    conn = httplib.HTTPSConnection("opendata.aemet.es")
    conn.request("GET", stationsUri)
    response = conn.getresponse()
    data = response.read()
    # data = data.split("\n",2)[1:]
    stations = xml.etree.ElementTree.fromstring(data)
    iter = stations.getiterator('ID')
    currentProv = provdict[key]
    # print currentProv
    for elem in iter:
        elementName = elem.tag
        elementText = elem.text
        # print elem.tag, elem.text
        idemas.append(elementText)
        #print elementText
        thisProv.append(currentProv)

# There are still some stations missing in the .xmls so I am manually adding them here

missingIdemas = ['1025A', '1109X', '5107D', '7012C', '8005X', '8416X', '9001S', '9562X', '9718X', '9783B', 'C619Y', 'C839X', 'C919K','4492F']

for i in missingIdemas:
    idemas.append(i)

missingProvs = ['Gipuzkoa', 'Santander', 'Granada', 'Murcia', 'València/Valencia', 'València/Valencia', 'Santander', 'Castelló/Castellón', 'Barcelona', 'Huesca', 'Las Palmas', 'Las Palmas', 'Santa Cruz de Tenerife', 'Badajoz']

for i in missingProvs:
    thisProv.append(i)

stationsdict = dict(zip(idemas,thisProv))

# This is where all the magic of getting and parsing all data reported in the last 24 hours from all stations happens

allLast24 = "/opendata/api/observacion/convencional/todas"

header = 'api_key: ' + apikey

uri = allLast24 + "?api_key=" + apikey

# Funny way of doing stuff, we first need to get an URL which then points to the actual data

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET",uri)
response = conn.getresponse()
data = response.read()
print data
predata = json.loads(data)

uriDatos = predata.get("datos")
uriMetadatos = predata.get("metadatos")

# Here we get the actual data

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET", uriDatos)
response = conn.getresponse()
data = response.read()
data = data.decode('iso-8859-1').encode('utf-8')
p = json.loads(data, 'utf-8')

# iterate through the individual data lines

counter = 0
creations = 0
updates = 0
failures = 0

for i in p:
    counter = counter + 1 # Just a variable to show the final number of records
    # I use a trick, so that when data from a station comes late, it will update with the latter
    # by calculating the ES ID with the Date and time (fint) and the Station ID (idema)
    # so... calcutating ElasticSearch ID
    datetime = i['fint'].encode('ascii', 'ignore')
    idema = i['idema'].encode('ascii', 'ignore')
    rawId = datetime + idema
    esId = filter(str.isalnum, rawId)
    # I want my data indexed by month, so we extract the month from datetime. Change the index name to your likings
    indexName = 'aemet-open-' + datetime[0:4] + '.' + datetime[5:7]
    # For all this to work, we need to define an index template in Elasticsearch.
    # See the file index_template.json for this. Of course, you need to adapt it to your index name

    # We also generate the geolocation field if you want to have nice maps...

    lat = str(i['lat']).encode('ascii', 'ignore')
    lon = str(i['lon']).encode('ascii', 'ignore')
    geolocation = lat + ',' + lon

    # We add the @timestamp and geolocation fields into the python list that will be indexed
    i['geolocation'] = geolocation
    i['@timestamp'] = datetime

    # We add the province from the stations dictionary generated above
    try:
        estaProv = stationsdict[idema]
        # print idema, estaProv
        i['provincia'] = estaProv
    except:
        # Warn if there is no province data for a given station so wwe can correct it
        print 'No Province info for ' + idema
    try:
        # we index the result in Elasticsearch
        res = es.index(index=indexName, doc_type='aemet', id=esId, body=i)
        # I want nice progress dots...
        sys.stdout.write('.')
        sys.stdout.flush()
        # and stats...
        if res[shards[result]] == 'created':
            creations = creations + 1
        elif res[shards[result]] == 'updated':
            updates = updates + 1
    except:
        # Something has failed so we need to take a closer look
        # print 'F',
        failures = failures + 1

# stats, stats...
print
print 'A total of ' + str(counter) + ' lines of data where received. ' + str(creations) + ' were created, ' + str(updates) + ' were updated and ' + str(failures) + ' ElasticSearach operations failed.'


