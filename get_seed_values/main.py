import googleapiclient.discovery
from google.cloud import bigquery

PROJECT_ID = "bergqvist-sandbox"
VERSION_NAME = "v3"
MODEL_NAME = "aclima"

service = googleapiclient.discovery.build('ml', 'v1')
name = 'projects/{}/models/{}'.format(PROJECT_ID, MODEL_NAME)
name += '/versions/{}'.format(VERSION_NAME)


def get_seed_values(month, day, hour):
    client = bigquery.Client()

    query = """
        SELECT AVG(bc) as bc, AVG(no120) as no120, AVG(co2) as co2 from `bergqvist-sandbox.oakland_aclima.reduction_two`
        WHERE month = {} and day = {} and hour = {}""".format(month, day, hour)

    query_job = client.query(query)

    result = query_job.result()

    for row in result:
        data = list(row.values())
        data.insert(0, month)
        return data


def get_prediction(request):
    seed_values = get_seed_values()
    response = service.projects().predict(
        name=name,
        body={'instances': [seed_values]}
    ).execute()
    if 'error' in response:
        print (response['error'])
    else:
        online_results = response['predictions']
    return str(online_results)
