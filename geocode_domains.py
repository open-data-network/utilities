import requests
from sodapy import Socrata
import json
# store api token in creds.txt
with open('creds.json', 'r') as f:
    creds = json.loads(f.read())
client = Socrata("odn.data.socrata.com", None, username=creds['username'], password=creds['password'])
data = requests.get('https://odn.data.socrata.com/resource/jwbj-xtgt.json?$select=:*,*&$where=region_name%20IS%20NOT%20NULL%20AND%20location%20IS%20NULL').json()
#{u'latitude': u'41.1085', u'needs_recoding': False, u'longitude': u'-117.6135'}
for row in data:
    location = requests.get('http://nominatim.openstreetmap.org/search/?q=%s,usa&format=json' % (row['region_name'])).json()[0]
    row['location'] = {'longitude': location['lon'], 'latitude': location['lat']} 
client.upsert("k53q-ytmx", data)
