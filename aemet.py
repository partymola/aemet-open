# AEMET Opendata parser
# Spain meteo Agecy AEMET has started an OpenData project...
# Before I was using the CSV files available, but, well, clumsy stuff

import httplib, urllib, ssl, json, requests, time

# Load the API key from file api.key
# Please note the API key has to be obtained at https://opendata.aemet.es/centrodedescargas/altaUsuario

ssl._create_default_https_context = ssl._create_unverified_context

with open ("api.key", "r") as conffile:
    conf=conffile.readlines()

apikey=conf[0]
#print apikey

header = 'api_key: ' + apikey


allStationsUri = "/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones"
station24hUri = "/opendata/api/observacion/convencional/datos/estacion/"

uri = allStationsUri + "?api_key=" + apikey

#print uri

conn = httplib.HTTPSConnection("opendata.aemet.es")
conn.request("GET",uri)
response = conn.getresponse()
data = response.read()
print data
predata = json.loads(data)

uriDatos = predata.get("datos")
uriMetadatos = predata.get("metadatos")

stations = requests.get(uriDatos, verify=False)
stationsDict = stations.json()

#debug: Print each station data as one line
a = 0
for p in stationsDict:
    # print p
    a = a+1
    print a,
    for key, value in p.items():
         print(key + ": " + value),

    # print '====================' #separator
    print

# get data for last 24h for each station

for station in stationsDict:
    requestUri = station24hUri + station['indicativo'] + "?api_key=" + apikey
    conn = httplib.HTTPSConnection("opendata.aemet.es")
    conn.request("GET", requestUri)
    response = conn.getresponse()
    data = response.read()
    # print data
    predata = json.loads(data)

    uriDatos = predata.get("datos")
    station24h = requests.get(uriDatos, verify=False)
    predata = station24h.json()

    for p in predata:
        # print type(p)
        # a = a + 1
        for a, b in p.items():
            print a, b
            #singleLine = line.json()
            #print line
        #except :
            #print "WRONG DATA"

        # print '====================' #separator
        #print
    time.sleep(10)
