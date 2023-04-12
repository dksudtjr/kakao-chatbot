import sys
sys.path.append('./libs') # 라이브러리 설치 경로 (Lambda에서 사용)
import requests
import base64
import json
import logging
import pymysql
from urllib import parse

from boto3.dynamodb.conditions import Key, Attr
import boto3  # aws sdk (DynamoDB, S3, EC2 등 생성/설정/관리 => 선행: aws configure)

# 환경변수
from dotenv import load_dotenv
import os
load_dotenv() # Parse a .env file and then load all the variables found as environment variables.

'''    
Spotify API - Header 생성 (access_token)
'''
def get_headers(client_id, client_secret):
    # "client_id:client_secret" => Base64 encoded string
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('ascii')
    headers = {"Authorization": "Basic {}".format(encoded)}
    data = {"grant_type":"client_credentials"}
    
    r = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data) # requests => headers, data에 dict타입
    access_token = json.loads(r.text)["access_token"]

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    return headers
    

'''
RDS row 삽입
'''
def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data)) # %s, %s, ... , %s
    columns = ', '.join(data.keys()) # id, name, ... , image_url
    key_placeholders = ', '.join([f'{k}=%s' for k in data.keys()]) # id=%s, name=%s, ...
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)

    cursor.execute(sql, 2*list(data.values())) # 동적으로 SQL문 구성 => execute("%s님은 %s되었습니다.", [값1, 값2]) 


'''
DynamoDB(top_tracks)에서 top_track 3개 가져오기
'''
def get_top_tracks(artist_id, artist_name):
    table = dynamodb.Table('top_tracks')
    response = table.query(
        KeyConditionExpression=Key('artist_id').eq(artist_id),
    )
    response['Items'].sort(key=lambda x: x['popularity'], reverse=True)

    kakao_items = []
    for top_track in response['Items'][:3]:
        top_track_name = top_track['name']
        # https://www.youtube.com/results?search_query=bts+dynamite
        youtube_url = 'https://www.youtube.com/results?' + parse.urlencode({'search_query': f'{artist_name} {top_track_name}'}, encoding='UTF-8', doseq=True)
        
        kakao_item = {
            "title": top_track_name,
            "description": top_track['album']['name'],
            "imageUrl": top_track['album']['images'][2]['url'], # 가장 낮은 해상도
            'link': {"web": youtube_url}
        }
        kakao_items.append(kakao_item)

    return kakao_items

'''
Kakao 응답 - Body 부분
'''
def kakao_body(outputs):
    return {
                "version": "2.0",
                "template": {
                    "outputs": outputs
                }
            }

'''
Kakao 응답 - Body - output - "listCard"
'''
def list_card(title, items, search_query):
    return  {
                "listCard": {
                    "header": {
                        "title": title
                    },
                    "items": items,
                    "buttons": [
                        {
                        "label": "더 보기",
                        "action": "webLink",
                        "webLinkUrl": 'https://www.youtube.com/results?' + parse.urlencode({'search_query': f'{search_query}'}, encoding='UTF-8', doseq=True)
                        }
                    ]
                }
            }

'''
Kakao 응답 - Body - output - "simpleText"
'''
def simple_text(text):
    return  {
                "simpleText": {
                    "text": f"{text}"
                }
            }

'''
Kakao 응답 - Body - output - "carousel"
'''
def carousel(items, card_type='listCard'):
    return {
        "carousel": {
            "type": card_type,
            "items": items # list_card()['listCard'] => Carousel에 들어갈 ListCard의 형태는 ListCard만 단독으로 보낼 때보다 한 단계 적음
        }
    }


# Spotify 앱 id/secret (Spotify for Developers - DASHBOARD - App)
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# RDS - DB(kakao-chatbot)
host = os.environ.get('host')
port = int(os.environ.get('port'))
database = os.environ.get('database')
username = os.environ.get('username')
password = os.environ.get('password')


# DynamoDB 접속 (선행: aws configure => IAM 인증)
try:
    dynamodb = boto3.resource('dynamodb')
except Exception:
    logging.exception("AWS DynamoDB에 접속할 수 없음")
    sys.exit(1)

def lambda_handler(event, context):

    # Spotify API - 헤더 (access_token)
    headers = get_headers(client_id, client_secret)

    # 사용자 발화
    request_body = json.loads(event['body'])
    utterance = request_body['userRequest']['utterance']

    # Spotify API(/search)에서 해당 가수의 id, name 받기
    params = {
        'q': utterance,
        'type': 'artist',
        'limit': '1',
    }

    response = requests.get("https://api.spotify.com/v1/search", headers=headers, params=params)
    spotify_raw = json.loads(response.text)

    # 에러 처리 - Spotify API(/search)에서 해당 가수를 찾을 수 없음
    if not spotify_raw['artists']['items']:
        body = kakao_body([
            simple_text('Spotify에서 해당 가수를 찾을 수 없습니다. 다시 입력해주세요.')
            ])
        return {
            'statusCode':200,
            'body': json.dumps(body),
            'headers': {
                'Access-Control-Allow-Origin': '*',
            }
        }   

    # Spotify API(/search)에서 해당 가수를 찾음
    spotify_artist_raw = spotify_raw['artists']['items'][0]
    artist_id, artist_name = spotify_artist_raw['id'], spotify_artist_raw['name']

    # AWS RDS 접속 - 메시지 반환 후, 연결을 끊기 때문에 시작할 때 매번 연결해야 함
    try:
        db = pymysql.connect(host=host, port=port, db=database, user=username, passwd=password, use_unicode=True, charset="utf8")
        cursor = db.cursor()
    except Exception:
        logging.exception("AWS RDS에 접속할 수 없음")
        sys.exit(1)

    # RDS(artists) 조회
    sql = f"SELECT id, name FROM artists WHERE id='{artist_id}'"
    cursor.execute(sql)
    rds_artist_raw = cursor.fetchall()
    

    # 1. RDS(artists)에 있으면, 관련 아티스트 추천
    if rds_artist_raw:

        query = f'''
            select artist_id from related_artists where artist_id = '{rds_artist_raw[0][0]}'
        '''
        cursor.execute(query)
        related_artists_artist_id = cursor.fetchall()

        # 1-1) RDS(related_artists)에 있으면, 관련 아티스트 추천
        if related_artists_artist_id:

            # carousel에 넣을 list_card 5개 (검색한 아티스트 1명 + 관련 아티트스 4명)
            list_cards = []
            # 검색한 아티스트 1명
            list_cards.append(list_card(artist_name, get_top_tracks(artist_id, artist_name), artist_name)['listCard'])
            
            # 관련 아티스트 4명 (distance 오름차순 정렬)
            related_artists_query = f'''
                select
                    y_artist, name, distance
                from
                    related_artists
                join
                    artists on related_artists.y_artist = artists.id
                where
                    artist_id = '{artist_id}'
                order by
                    distance ASC
                limit 4
            '''
            cursor.execute(related_artists_query)
            related_artists = cursor.fetchall()

            for related_artist in related_artists:
                list_cards.append(list_card(related_artist[1], get_top_tracks(related_artist[0], related_artist[1]), related_artist[1])['listCard'])

            # 최종 메시지
            body = kakao_body(
                [
                    simple_text(f"{artist_name}, 관련 아티스트들의 노래를 들어보세요🎵"),
                    carousel(list_cards, 'listCard')
                ]
            )

            # RDS 연결 끊기
            db.close()

            return {
                'statusCode':200,
                'body': json.dumps(body),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                }
            }
        
        # 1-2) RDS(related_artists)에 없으면, top_tracks 응답
        else:
            r = requests.get(f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks', headers=headers, params={'country': 'KR'})
            raw = json.loads(r.text)

            kakao_items = []
            for track in raw['tracks'][:3]:
                kakao_item = {
                    "title": track['name'],
                    "description": track['album']['name'],
                    "imageUrl": track['album']['images'][2]['url'], # 가장 낮은 해상도
                    'link': {"web": 'https://www.youtube.com/results?' + parse.urlencode({'search_query': f'{artist_name} {track["name"]}'}, encoding='UTF-8', doseq=True)}
                }
                kakao_items.append(kakao_item)

            # 최종 메시지
            body = kakao_body([
                simple_text(f'해당 아티스트의 관련 아티스트 추천은 새벽 3시에 업데이트됩니다. 잠시 기다려주세요😥'),
                list_card(artist_name, kakao_items, artist_name)
            ])

            return {
                'statusCode':200,
                'body': json.dumps(body),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                }
            }



    # 2. RDS에 없으면, RDS(artists), DynamoDB(top_tracks) 저장 후, top_tracks 리턴
    else:

        # 2-1) DynamoDB(top_tracks) 비동기 저장
        lambda_client = boto3.client('lambda')
        response = lambda_client.invoke(
            FunctionName='top-tracks',
            InvocationType='Event', # 비동기
            Payload=json.dumps({'artist_id': artist_id})
        )
        
        # 2-2) RDS(artists) 저장
        artist = {
            'id': artist_id,
            'name': artist_name,
            'followers': spotify_artist_raw['followers']['total'],
            'popularity': spotify_artist_raw['popularity'],
            'url': spotify_artist_raw['external_urls']['spotify'],
            'image_url': spotify_artist_raw['images'][0]['url']
        }

        # AWS RDS 업데이트
        insert_row(cursor, artist, 'artists')

        # AWS RDS 변경사항 적용 후, 연결 끊기
        db.commit()
        db.close()

        r = requests.get(f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks', headers=headers, params={'country': 'KR'})
        raw = json.loads(r.text)

        kakao_items = []
        for track in raw['tracks'][:3]:
            kakao_item = {
                "title": track['name'],
                "description": track['album']['name'],
                "imageUrl": track['album']['images'][2]['url'], # 가장 낮은 해상도
                'link': {"web": 'https://www.youtube.com/results?' + parse.urlencode({'search_query': f'{artist_name} {track["name"]}'}, encoding='UTF-8', doseq=True)}
            }
            kakao_items.append(kakao_item)

        # 최종 메시지
        body = kakao_body([
            simple_text(f'해당 아티스트({artist_name})가 추가되었습니다🎉 (관련 아티스트 추천은 새벽 3시에 업데이트됩니다. 잠시 기다려주세요)'),
            list_card(artist_name, kakao_items, artist_name)
        ])

        return {
            'statusCode':200,
            'body': json.dumps(body),
            'headers': {
                'Access-Control-Allow-Origin': '*',
            }
        }