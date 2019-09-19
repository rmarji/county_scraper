#!/usr/bin/env python

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

from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests


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

    return response.json()


def lambda_handler(event, context):
    '''Main entry point for Lambda function'''
    permits = get_list_of_permits("2019-07-01", "2019-07-31")
    with open('santa_clara_output.json', 'w') as jsonfile:
        for permit in permits:
            permit_id = permit['csmCaseno']
            details = get_permit_details(permit_id)
            permit_json = {
                'name': permit.get('csmNameLast'),
                'address': permit.get('csmAddress'),
                'application_date': permit.get('csmRecdDate'),
                'project_name': permit.get('csmProjname'),
                'status': permit.get('csmStatus'),
                'description': permit.get('csmDescription'),
                'permit_id': permit_id,
            }
            activities = []
            for activity in details:
                activities.append({
                    'title': activity.get('actionDescription'),
                    'assigned_to': activity.get('csaAssignedTo'),
                    'done_by': activity.get('csaDoneBy'),
                    'status': activity.get('csaDisp'),
                    'date_requested': activity.get('csaDate1'),
                    'target_date': activity.get('csaDate2'),
                    'received_date': activity.get('csaDate3'),
                })
            permit_json['activities'] = activities
            permit_json['county'] = 'Santa Clara'

            es_client.index(index='county_records', body=permit_json)


if __name__ == "__main__":
    lambda_handler(None, None)
