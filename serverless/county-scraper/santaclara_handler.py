#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
#AWS Console https://117915020299.signin.aws.amazon.com/console
#Kibana https://search-county-records-mbz3w42c3ehmvsjcfb4tb2orly.us-east-1.es.amazonaws.com/_plugin/kibana/app/kibana#/discover?_g=()
#PRD https://onedrive.live.com/view.aspx?resid=B78BA86445BEEDF8!128&authkey=!AEAu8MLR9VyL4Vs
'''
Website URL:   https://smartpermit.santaclaraca.gov:8443/apps/cap_sc/#/lookup


Entering the site: input date range and select lookup information

Initial sync: input data range of {01-01-2019 , 08-07-2019}

Pull data on a daily cadence after that.

Example:{ 08-08-2019,  08-08-2019}
'''
'''
Name	Permit #	Address	Applicant Date	Project Name	Status	Description	Title	Assigned	Done By	Status	Date Requested	Target Date	Received
'''
import json

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import requests
import pandas as pd
import geocoder
from datetime import datetime, date


# In[2]:


REQUEST_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    'Host': 'smartpermit.santaclaraca.gov:8443',
    'Origin': 'https://smartpermit.santaclaraca.gov:8443',
    'Referer': 'https://smartpermit.santaclaraca.gov:8443/apps/cap_sc/',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
}

es_client = Elasticsearch(
    hosts=[{
        'host': 'search-county-records-mbz3w42c3ehmvsjcfb4tb2orly.us-east-1.es.amazonaws.com',
        'port': 443
    }],
    connection_class=RequestsHttpConnection,
    verify_certs=True,
    use_ssl=True
)



mapping = {
  "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings" : {
      "row" : {
        "properties" : {
          "@timestamp" : {
            "type" : "date"
          },
          "address" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "application_date" : {
            "type" : "date"
          },
          "assigned_to" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "date_requested" : {
            "type" : "date"
          },
          "description" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "done_by" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "geopoint" : {
            "type": "geo_point"
          },
          "name" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "permit_id" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "permit_id_activity" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "project_name" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "received_date" : {
            "type" : "date"
          },
          "status" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "status_activity" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
          "target_date" : {
            "type" : "date"
          },
          "title" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          }
        }
      }
    }
  }

#es_client.indices.create(index='county_permit_activities_santa_clara', ignore=400, body=mapping)


# In[3]:


q = {
  "query": {
    "match_all": {}
  }
}
#es_client.delete_by_query('county_permit_activities_*',q)


# In[4]:


def get_list_of_permits(start_date, end_date):
    ''' '''
    post_data = {
        "srvcName": "angCpLookup",
        "srvcParams": json.dumps({
            "siteId":  0,
            "type":  2,
            "procCode":  None,
            "projectName":  "undefined",
            "procStatus":  None,
            "personFname":  None,
            "personLname":  None,
            "addrStNum":  None,
            "addrStDir":  None,
            "addrStName":  None,
            "addrStUnit":  None ,
            "startDate":  start_date,
            "endDate":  end_date
        })
    }

    response = requests.post(
        'https://smartpermit.santaclaraca.gov:8443/CAP-EXT-SC/jaxrs/services/op/execute',
        json=post_data, headers=REQUEST_HEADERS
    )

    return response.json()


# In[5]:


def get_permit_details(permit_id):
    ''' '''
    post_data = {
        "srvcName": "angCpLookupEventList",
        "srvcParams": json.dumps({
            "mode": "0",
            "userId": "-1",
            "siteId": "0",
            "procRecId": permit_id
        })
    }

    response = requests.post(
        "https://smartpermit.santaclaraca.gov:8443/CAP-EXT-SC/jaxrs/services/op/execute",
        json=post_data, headers=REQUEST_HEADERS
    )
    #print(x)
    return response.json()


# In[6]:


def get_permits_df(permits):
    permits_df_org = pd.DataFrame(permits)
    permits_df = permits_df_org[['csmCaseno','csmNameLast','csmAddress','csmRecdDate','csmProjname','csmStatus','csmDescription']].copy()
    permits_df.columns = ['permit_id','name','address','application_date','project_name','status','description']
    permits_df.index = permits_df.permit_id
    print('Number of permits:', permits_df.shape)
    return permits_df


# In[7]:


#get_permit_details('FIR2019-01146')


# In[8]:


def get_activites_df(permits_df):
    activities_list = []
    columns = ['permit_id','activity_id','title','assigned_to','done_by','status','date_requested','target_date','received_date']
    #loop through permits and request activties
    for permit in permits_df.itertuples():
        permit_id = permit.permit_id
        activities_json = get_permit_details(permit.permit_id)
        #parse
        activities = pd.DataFrame(activities_json)
        
        #Handle no activities
        if len(activities) > 0:
            activities['permit_id'] = permit_id

            activities =  activities[['permit_id','csaId','actionDescription','csaAssignedTo','csaDoneBy','csaDisp','csaDate1','csaDate2','csaDate3']]
            activities.columns = columns
        else:
            activities = pd.DataFrame(columns=columns)
            activities.append({'permit_id':permit_id},ignore_index=True)
            print('No Activities For This Permit:',permit_id)
        
        # append the activities for that specific permit to the list
        activities_list.append(activities)

    # combine all activities
    activities_df = pd.concat(activities_list)
    # Join the permit data to the list
    activities_dfs = activities_df.join(permits_df,on='permit_id',lsuffix='_activity',how='outer')
    
    activities_dfs.received_date = pd.to_datetime(activities_dfs.received_date)
    
    return activities_dfs


# In[9]:


# Geo code addresses
def geocode(address):
    #https://github.com/googlemaps/google-maps-services-python/blob/master/googlemaps/geocoding.py
    g = geocoder.google(address,key='AIzaSyCviHP1Wy9vzKgVRejvisQvFsMbzj8MwV0',)
    if g.lng is None or g.lat is None:
        # Santa Clara GPS COORDS
        geopoint = [-121.9552356 , 37.3541079]
    else:
        geopoint = [g.lng,g.lat]
    return geopoint


# In[10]:


# TODO creat e an index and an index mapping
# Go through the list of activities and add new metadata for elastic search then bulk index them
def index_results(es,docs,index,timestamp_col,id_col,daily_index=False):
    #Handle the columns
    #docs.columns = [col.replace('_','.') for col in docs.columns]

    # this is useful to create an index per day
    docs['_index'] = index
    if daily_index == True:
        docs['_index'] = docs['_index'] + docs[timestamp_col].dt.strftime('%Y%m%d')
    # this is for backwards compatibility
    docs['_type'] = "row"
    # the main attribuite that we will filter dates by.
    docs['@timestamp'] = docs[timestamp_col]

    # TODO find a cleaner way
    docs['_id'] = docs[id_col]

    #docs = docs.drop(labels='.id',axis='columns')


    # transform each report into a dictionary for ES to understand it
    docs_dict = docs.to_dict(orient='records')

    # index each report in bulk instead of one row at a time to make it faster the default operation is indexing
    esResponse = helpers.bulk(es,docs_dict,stats_only=True)
    print(esResponse)


# In[11]:


def clean_activities(activities):
    bigbang = datetime(1970,1,1,0,0)
    values = {'received_date': bigbang, 'target_date': bigbang,'date_requested':bigbang, 'assigned_to': 'Unknown' , 'project_name': 'Unknown','done_by': 'Unknown','status_activity':'Unknown'}
    activities.fillna(values,inplace=True)
    #activities.drop('permit_id_activity',axis=1,inplace=True)
    activities.dropna(inplace=True)
    print('number of activties in total:', activities.shape)
    return activities


# In[12]:


##activities.to_excel('./activities.xlsx')


# In[13]:



ac_l = []


# In[18]:


def lambda_handler(event, context):
    '''Main entry point for Lambda function'''

    #TODO add retro active mode

    today = date.today()
    today_s = today.strftime("%Y-%m-%d")

    month = '9'
    # Use when retroactivally importing data
    #start_date = '2019-0'+month+'-1'
    #end_date =  '2019-0'+month+'-31'

    start_date = today_s
    end_date =  today_s

    permits = get_list_of_permits(start_date, end_date)

    permits_df = get_permits_df(permits)
    permits_df['geopoint'] = permits_df.address.apply(geocode)

    activities = get_activites_df(permits_df)
    activities = clean_activities(activities)


    activities['county'] = 'Santa Clara'

    #activities.to_pickle('./data/activities'+month+'.pkl')
    #activities.to_csv('./data/activities'+month+'.csv')
    #pickle
    # csv
    #index data
    #es_client.index(index='county_permit_activities_santa_clara', body=permit_json)
    print('records added to Elasticsearch:')
    response = index_results(es_client,activities,index='county_permit_activities_santa_clara',timestamp_col='application_date',id_col='activity_id')
    


# In[15]:


def historical_import():
    ac_l = []
    for i in range(1,10):
        ac_l.append(pd.read_pickle('./data/activities'+ str(i) +'.pkl'))

    h2019 = pd.concat(ac_l)

    h2019.to_csv('./data/activities2019.pkl')
    
    h2019.date_requested = pd.to_datetime(h2019.date_requested)
    h2019.received_date = pd.to_datetime(h2019.received_date)
    h2019.application_date = pd.to_datetime(h2019.application_date)
    h2019.target_date = pd.to_datetime(h2019.target_date)
    h2019.geopoint


# In[16]:


#index_results(es_client,h2019,index='county_permit_activities_santa_clara',timestamp_col='application_date',id_col='activity_id')


# In[ ]:


if __name__ == "__main__":
    lambda_handler(None, None)


# In[ ]:




