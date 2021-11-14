# code reference:
# https://docs.aws.amazon.com/opensearch-service/latest/developerguide/search-example.html

import json
import requests
import boto3
from requests_aws4auth import AWS4Auth
import time

lex = boto3.client("lexv2-runtime")
valid_labels = ["Cat", "Dog", "Ball"]
NUM_RESULTS_PER_LABEL = 3
S3_URL = "https://photosryan.s3.amazonaws.com/"
transcribe = boto3.client("transcribe")

def postText(message):
    response = lex.recognize_text(
        botId='O3AFY3AS0A',
        botAliasId='6S6KGTJWNV',
        localeId='en_US',
        sessionId = "session1",
        text=message,
    )
    return response

def lambda_handler(event, context):
    message = event["queryStringParameters"]["q"]
    if message == "audio":
        message = transcribeAudio("audio")
    print("message: " + message)
    res = postText(message)
    print("response from lex: ", res)
    labels = []
    intent = res["interpretations"][0]['intent']
    if intent['name'] == "SearchIntent":
        for label in intent['slots']['photoType']['values']:
            labels.append(label['value']['interpretedValue'])
    print("searching for labels:", labels)
    res = getElasticSearch(labels)
    print("res from opensearch: ", res)
    return {
        "isBase64Encoded": False,
		'statusCode': 200,
		'headers': {
			"Access-Control-Allow-Origin": "*",
			"Access-Control-Allow-Headers": "*",
			'Content-Type': 'application/json'
		},
		'body': str(res)
	}

def getElasticSearch(labels):
    credentials = boto3.Session().get_credentials()
    service = "es"
    auth = AWS4Auth(credentials.access_key, credentials.secret_key, "us-east-1", service, session_token=credentials.token)
    host = "https://search-photos-6ioq3375vyqygdfx4gi76a6pvy.us-east-1.es.amazonaws.com/photos/_search"
    res = []
    for label in labels:
        query = {
            "query": {
                "multi_match": {
                    "query": label
                    }
                }
            }
        # Elasticsearch 6.x requires an explicit Content-Type header
        headers = { "Content-Type": "application/json" }
    
        # Make the signed HTTP request
        r = requests.get(host, auth=auth, headers=headers, data=json.dumps(query))
        print(r.json())
        hits = r.json()["hits"]
        count = 0
        for hit in hits["hits"]:
            if count >= NUM_RESULTS_PER_LABEL:
                break
            res.append(S3_URL + hit["_source"]["object_key"])
            count += 1
    return res
    
def transcribeAudio(audioName):
    jobName = str(time.time())
    uri = "https://transcriberyan.s3.amazonaws.com/" + audioName + ".wav"
    transcribe.start_transcription_job(TranscriptionJobName=jobName,Media={'MediaFileUri': uri},MediaFormat='wav',LanguageCode='en-US')
    ready = False
    while not ready:
        response = transcribe.get_transcription_job(TranscriptionJobName=jobName)
        if response['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            ready = True
        time.sleep(5)
    print(response)
    textUri = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
    textJson = requests.get(textUri).json()
    print(textJson)
    text = textJson["results"]["transcripts"][0]["transcript"]
    print(text)
    return text