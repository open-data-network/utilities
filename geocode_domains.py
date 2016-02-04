import requests
from sodapy import Socrata
import json
# store api token in creds.txt
with open('creds.json', 'r') as f:
    creds = json.loads(f.read())
client = Socrata("odn.data.socrata.com", creds["token"], username=creds['username'], password=creds['password'])
data = requests.get('https://odn.data.socrata.com/resource/jwbj-xtgt.json?$select=:*,*&$where=region_name%20IS%20NOT%20NULL%20AND%20location%20IS%20NULL').json()
#{u'latitude': u'41.1085', u'needs_recoding': False, u'longitude': u'-117.6135'}
for row in data:
    if len(row['region_name'].split(',')) > 2:
        location = requests.get('http://nominatim.openstreetmap.org/search/?q=%s&format=json' % (row['region_name'])).json()
    else:
        location = requests.get('http://nominatim.openstreetmap.org/search/?q=%s,usa&format=json' % (row['region_name'])).json()
    if location:
        location = location[0]
        row['location'] = "(%s, %s)" % (location['lat'], location['lon'])
client.upsert("k53q-ytmx", data)
