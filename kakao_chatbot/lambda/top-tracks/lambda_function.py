'''
Lambda(kakao-chatbo)에서 보낸 event({'artist_id': artists_id})를 받아서,
Spotify API로 보내고,
받은 응답(top_tracks)을 DynamoDB에 저장
'''

import sys
sys.path.append('./libs') # 라이브러리 설치 경로 (Lambda에서 사용)
import requests
import base64
import json
import logging

import boto3

# 환경변수
from dotenv import load_dotenv
import os
load_dotenv() # Parse a .env file and then load all the variables found as environment variables.

'''    
Spotify API - Header 생성 (access_token)
'''
def get_headers(client_id, client_secret):
    # "client_id:client_secret" => Base64 encoded string이어야 함
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('ascii')
    headers = {"Authorization": "Basic {}".format(encoded)}
    data = {"grant_type":"client_credentials"}
    
    r = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data) # requests => headers, data에 dict타입
    access_token = json.loads(r.text)["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    return headers


# Spotify 앱 id/secret (Spotify for Developers - DASHBOARD - App)
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# DynamoDB 접속 (선행: aws configure 명령을 통해 access_key 설정)
try:
    dynamodb = boto3.resource('dynamodb')
except Exception:
    logging.exception("AWS DynamoDB에 접속할 수 없음")
    sys.exit(1)

'''
Lambda(kakao-chatbot)가 트리거 (event = {'artist_id': '아티스트ID'})
'''
def lambda_handler(event, context):
    # Spotify API 헤더(access_token)
    headers = get_headers(client_id, client_secret)

    # 1. event로 받은 "아티스트 id"를 Artists API로 보내고
    r = requests.get(f'https://api.spotify.com/v1/artists/{event["artist_id"]}/top-tracks', headers=headers, params={'country': 'KR'})
    raw = json.loads(r.text)

    # dynamoDB - top_tracks 테이블 사용
    table = dynamodb.Table('top_tracks') # boto3 문법
    
    # 2. ["아티스트 id" + 받은 응답]을 dynamoDB - top_tracks 테이블에 저장
    with table.batch_writer() as batch:
        for track in raw['tracks'][:3]:
            item = {'artist_id': event["artist_id"]}
            item.update(track)
            batch.put_item(Item=item) # boto3 - DynamoDB (PK에 따라 아이템 삽입/대체)

    return 'SUCCESS'