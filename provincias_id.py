# AEMET Opendata parser
# Spain meteo Agecy AEMET has started an OpenData project...
# Before I was using the CSV files available, but, well, clumsy stuff
# There is no full list of the Station IDs by province, so we build one from data
# available in the dropdowns on https://opendata.aemet.es/centrodedescargas/productosAEMET?
# Provinces are in https://opendata.aemet.es/centrodedescargas/xml/provincias.xml
# Then for each province there is a file https://opendata.aemet.es/centrodedescargas/xml/udat/udat[ID].xml

import httplib, urllib, ssl, json, requests, time
import sys
import xml.etree.ElementTree

reload(sys)
sys.setdefaultencoding('utf8')


# Load the API key from file api.key
# Please note the API key has to be obtained at https://opendata.aemet.es/centrodedescargas/altaUsuario

ssl._create_default_https_context = ssl._create_unverified_context

#print uri

provinciasUri = '/centrodedescargas/xml/provincias.xml'

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET",provinciasUri)
response = conn.getresponse()
data = response.read()
# print data

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
    ifemas = []
    for elem in iter:
        elementName = elem.tag
        elementText = elem.text
        # print elem.tag, elem.text
        ifemas.append(elementText)

    iter = stations.getiterator('NOMBRE')
    nombres = []
    for elem in iter:
        elementName = elem.tag
        elementText = elem.text
        # print elem.tag, elem.text
        nombres.append(elementText)

    stationsdict = dict(zip(ifemas,nombres))

    for i in stationsdict:
        a = a + 1
        print a,
        print i, stationsdict[i]

    # print key, provdict[key], stationsUri



