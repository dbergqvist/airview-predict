import googleapiclient.discovery
from google.cloud import bigquery

PROJECT_ID = "bergqvist-sandbox"
VERSION_NAME = "v3"
MODEL_NAME = "aclima"

service = googleapiclient.discovery.build('ml', 'v1')
name = 'projects/{}/models/{}'.format(PROJECT_ID, MODEL_NAME)
name += '/versions/{}'.format(VERSION_NAME)


def get_seed_values(month, day, hour, zipcode):
    client = bigquery.Client()

    query = """
        SELECT AVG(bc) as bc, AVG(no120) as no120, AVG(co2) as co2 from `bergqvist-sandbox.oakland_aclima.reduction_two`
        WHERE month = {} and day = {} and hour = {} and zipcode = '{}'""".format(month, day, hour, zipcode)

    query_job = client.query(query)

    result = query_job.result()

    for row in result:
        data = list(row.values())
        data.insert(0, month)
        data.insert(len(data), zipcode)
        #print(data, 'bla')
    return data


def get_prediction(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    if request.method == 'OPTIONS':
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    if request_json:
        month = request_json['month']
        day = request_json['day']
        hour = request_json['hour']
        zipcode = request_json['zipcode']
    elif request_args:
        month = request_args['month']
        day = request_args['day']
        hour = request_args['hour']
        zipcode = request_args['zipcode']
    else:
        print('Please provide seed values')

    seed_values = get_seed_values(month, day, hour, zipcode)
    print(seed_values)
    response = service.projects().predict(
        name=name,
        body={'instances': [seed_values]}
    ).execute()
    if 'error' in response:
        print (response['error'])
    else:
        online_results = response['predictions']
    return (str(online_results), 200,  headers)
