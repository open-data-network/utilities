import requests
from joblib import Parallel, delayed  
import multiprocessing
import traceback
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
datasets_of_datasets = dict([(row["nbe_id"], row) for row in requests.get('https://opendata.socrata.com/resource/v7cx-idrg.json?$limit=25000').json()])
def get_nested_value(nested_dict, first, second):
    if nested_dict.get(first):
        if nested_dict.get(first).get(second):
            return nested_dict.get(first).get(second)
        else:
            return None
    else:
        return None
num_cores = multiprocessing.cpu_count()

import dateutil
import dateutil.parser
from pytz import timezone
from datetime import date
from datetime import datetime
tz = timezone('America/Los_Angeles')

def run_count(i, domain, theid, app_token):
    api_url = 'https://%s/resource/%s.json' % (domain, theid)
    count_url = '%s?$select=count(*)&$$app_token=%s' % (api_url, app_token)
    count_data = None
    try:
        count_data = requests.get(count_url, verify=False, timeout=2).json()
        number_of_rows = int(count_data[0][count_data[0].keys()[0]]) # sometimes key is count_1 instead of count
        print (i, theid, number_of_rows)
        return (theid, number_of_rows)
    except Exception, err:
        print count_url
        print count_data, traceback.print_exc()
        return (theid, None)

def do():
    app_token = creds['token']
    results = []
    print 'doing api.us.socrata'
    for i in range(10):
        results.extend(requests.get('http://api.us.socrata.com/api/catalog/v1?only=datasets&limit=10000&offset='+str(10000*i)).json()['results'])
    print 'getting existing data'
    inputs = []
    existing_data = requests.get('https://opendata.socrata.com/resource/v7cx-idrg.json?$select=domain,nbe_id&$limit=30000').json()
    for i, row in enumerate(existing_data):
        inputs.append([i, row['domain'], row['nbe_id'], creds['token']])
    counts = dict(Parallel(n_jobs=num_cores)(delayed(run_count)(*inp) for inp in inputs))
    data = results
    print 'number_of_datasets', len(data)
    modified_data = []
    for i, row in enumerate(data):
        print 'ROW', i
        d = {}
        new_d = {}
        for key in row.keys():
            if isinstance(row[key], dict):
                d.update(row[key])
            else:
                d[key] = row[key]
        new_d['nbe_id'] = d.get("nbe_fxf")
        if not new_d['nbe_id']:
            continue
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
            new_d['tags'] = ','.join(['"%s"' % (item) for item in d['tags'] if item])
        else:
            del new_d['tags']
        try:
            new_d["page_views_last_week"] = d["view_count"]["page_views_last_week"]
            new_d["page_views_total"] = d["view_count"]["page_views_total"]
            new_d["page_views_last_month"] = d["view_count"]["page_views_last_month"]
        except:
            pass
        
        if not get_nested_value(datasets_of_datasets, new_d['nbe_id'], 'created_at'):
            print 'getting created at time'
            try:
                new_d["created_at"] = requests.get('https://'+new_d['domain']+'/api/views/'+new_d['nbe_id']+'.json', timeout=2).json()["createdAt"]
            except:
                pass
        new_d["updated_at"] = d["updatedAt"]
        if not get_nested_value(datasets_of_datasets, new_d['nbe_id'], 'columns'):
            print 'getting columns'
            try:
                new_d["columns"] = requests.get('https://'+new_d['domain']+'/api/views/'+new_d['nbe_id']+'.json', timeout=2).json()["columns"]
                new_d["number_of_columns"] = len(new_d['columns'])
                new_d['columns'] = json.dumps(new_d['columns'])
            except:
                pass
        new_d["updated_at"] = d["updatedAt"]
        if new_d['nbe_id']:
            
            new_d['api_url'] = 'https://%s/resource/%s.json' % (new_d['domain'], new_d['nbe_id'])
            new_d['frontend_url'] = 'https://%s/resource/%s' % (new_d['domain'], new_d['nbe_id'])
            print new_d['api_url']
            if counts.get(new_d['nbe_id']):
                new_d['number_of_rows'] = counts.get(new_d['nbe_id'])
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
            except Exception,e:
                print e
    for i in range(loops):
        print 'loop', i
        keep_trying(modified_data[i*200:(i+1)*200])
    keep_trying(modified_data[(i+1)*200:])

while True:
    do()
