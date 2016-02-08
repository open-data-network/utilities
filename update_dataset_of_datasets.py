import requests
from sodapy import Socrata
import json
# store api token in creds.txt
with open('creds.json', 'r') as f:
    creds = json.loads(f.read())
client = Socrata("opendata.socrata.com", creds["token"], username=creds['username'], password=creds['password'])
domain_data = requests.get('https://odn.data.socrata.com/resource/jwbj-xtgt.json?$select=domain,location&$where=location%20IS%20NOT%20NULL').json()
domain_to_location = dict([(item['domain'], item['location']) for item in domain_data])
from joblib import Parallel, delayed  
import multiprocessing
import traceback
import requests

num_cores = multiprocessing.cpu_count()*20

import dateutil
import dateutil.parser
from pytz import timezone
from datetime import date
from datetime import datetime
tz = timezone('America/Los_Angeles')

def run_count(i, theid, api_url, app_token, tables_list, d):
    conn = r.connect( "localhost", 28015).repl()
    count_url = '%s?$select=count(*)&$$app_token=%s' % (api_url, app_token)
    count_data = None
    try:
        count_data = requests.get(count_url, verify=False).json()
        
        number_of_rows = int(count_data[0][count_data[0].keys()[0]]) # sometimes key is count_1 instead of count
        
        r.db('public').table('datasets').get(theid).update({"number_of_rows": int(number_of_rows), "is_number_of_rows_error": False}).run(conn, noreply=True)
        print i, theid, int(number_of_rows)
    except Exception, err:
        r.db('public').table('datasets').get(theid).update({"is_number_of_rows_error": True, "number_of_rows_error": traceback.format_exc()}).run(conn, noreply=True)
        print count_url
        print count_data, traceback.print_exc()
    url = '%s?$select=:created_at&$order=:created_at&$limit=1&$$app_token=%s' % (api_url, app_token)
    try:
        created_at = requests.get(url).json()[0][':created_at']
        r.db('public').table('datasets').get(theid).update({"created_at": created_at}).run(conn, noreply=True)
    except Exception, err:
        print url, traceback.print_exc()


app_token = creds['token']
results = []
for i in range(10):
    results.extend(requests.get('http://api.us.socrata.com/api/catalog/v1?only=datasets&limit=10000&offset='+str(10000*i)).json()['results'])
data = results
print 'number_of_datasets', len(data)
modified_data = []
for i, row in enumerate(data):
    print i
    d = {}
    new_d = {}
    for key in row.keys():
        if isinstance(row[key], dict):
            d.update(row[key])
        else:
            d[key] = row[key]
    new_d['nbe_id'] = d.get("nbe_fxf")
    new_d['domain'] = d['domain']
    if domain_to_location.get(d["domain"]):
        new_d["domain_location"] = domain_to_location.get(d["domain"])
        new_d["domain_location"] = new_d["domain_location"]["coordinates"][::-1]
        new_d["domain_location"] = "(%s)" % (str(new_d["domain_location"])[1:-1])
    new_d['name'] = d['name']
    new_d['description'] = d['description']
    new_d['categories'] = d['categories'] + [d['domain_category']]
    if new_d['categories']:
        new_d['categories'] = ','.join(['"%s"' % (item) for item in new_d['categories'] if item])
    else:
        del new_d['categories']
    new_d['tags'] = d['tags'].extend(d['domain_tags'])
    if new_d['tags']:
        new_d['tags'] = ','.join(['"%s"' % (item) for item in new_d['tags'] if item])
    else:
        del new_d['tags']
    if new_d['nbe_id']:
        modified_data.append(new_d)
print len(modified_data)
print "upserting"
print str(modified_data)[:2000]
loops = len(modified_data)/200
def keep_trying(data):
    while True:
        try:
            client.upsert("v7cx-idrg", data)
            return
        except:
            pass
for i in range(loops):
    print 'loop', i
    keep_trying(modified_data[i*200:(i+1)*200])
keep_trying(modified_data[(i+1)*200:])
