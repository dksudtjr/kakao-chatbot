'''
[S3에 Parquet 업로드 (top_track, audio_features)]

1. RDS(artists)에서 artist_id 가져옴
2. Spotify API에서 top_tracks, audio_features 가져옴 (추가로, DynamoDB(top_tracks) 업데이트)
3. 로컬에 top_tracks, audio_features 저장 (parquet 포맷)
4. S3에 top_tracks, audio_features 저장 (parquet 포맷)

[RDS - related_artists 추가]

Athena를 통해 top_tracks, audio_features 계산 후, RDS(related_artists) 업데이트

5. Athena
    5-0) (최초 1회 실행) [Athena] external 테이블 생성 (top_tracks, audio_features)
    5-1) [Athena] "4. S3에 top_tracks, audio_features 저장"에서 생성한 파티션을 Athena 테이블에 업데이트 
        => Athena에서 인식 후, 쿼리

    5-2)    [Athena] 아티스트별 audio_features (평균) - top_tracks, audio_features
            [Athena] audio_features 최소값/최대값 - audio_features
    5-3) RDS(related_artists)에 관련 아티스트 저장


'''

# requirements.txt => requests, pymysql, jsonpath, pandas, pyarrow

import sys
sys.path.append('./libs') # 라이브러리 설치 경로 (Lambda에서 사용)
import base64
import requests
import json
import logging
import pymysql
import jsonpath
import pandas as pd
import pyarrow
import os
import datetime
import boto3
import time
import math

# 환경변수
from dotenv import load_dotenv
import os
load_dotenv() # Parse a .env file and then load all the variables found as environment variables.
s3_bucket = os.environ.get('s3')
athena_db = os.environ.get('athena_db')

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


# 1) Athena 쿼리 실행
def query_execution(query, athena):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': f'{athena_db}' # Athena에서 생성한 DB (External테이블 존재)
        },
        ResultConfiguration={
            'OutputLocation': f's3://{s3_bucket}/athena-query-results/',  # 쿼리 결과 저장하는 위치
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3' # S3 기본 암호화
            }
        }
    )

    return response

# 2) Athena 쿼리 결과
def get_query_results(query_id, athena):

    # 쿼리 실행 정보(상태)
    response = athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )

    # 해당 쿼리 완료될 때까지 대기
    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(5)
        response = athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )
    
    # 쿼리 결과
    response = athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000 # 가져올 결과의 최대 수 (Athena는 최대 1000개)
    )

    return response


''' 

response['ResultSet']['Rows'] =[
    {
        "Data": [
            {'VarCharValue': '칼럼1'},
            {'VarCharValue': '칼럼2'}, ...
        ] 
    },
    {
        "Data": [
            {'VarCharValue': '값1'},
            {'VarCharValue': '값2'}, ...
        ]
    },
    {
        "Data": [
            {'VarCharValue': '값1'},
            {'VarCharValue': '값2'}, ...
        ]
    },
]
'''

# 3) Athena 쿼리 결과 전처리  =>  [{결과1}, {결과2}, ...]
def process_response(response):

    # 컬럼명 ['칼럼1', '칼럼2', ...]
    columns = [col['VarCharValue'] for col in response['ResultSet']['Rows'][0]['Data']] #  0번째는 칼럼정보

    results = []
    for result in response['ResultSet']['Rows'][1:]: # 1번째부터 쿼리 결과
        values = [] # ['값1', '값2', ...]
        for value in result['Data']:
            try:
                values.append(value['VarCharValue'])
            except:
                values.append('')
        results.append(dict(zip(columns, values))) # {'칼럼1': '값1', '칼럼2': '값2', ...}
    
    return results # [{결과1}, {결과2}, ...]



# 정규화 계산 함수 => 데이터 범위 [0, 1]
def normalize(x, x_min, x_max):

    normalized = (x-x_min) / (x_max-x_min)
    return normalized

'''
RDS row 삽입
'''
def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data)) # %s, %s, ... , %s
    columns = ', '.join(data.keys()) # id, name, ... , image_url
    key_placeholders = ', '.join([f'{k}=%s' for k in data.keys()]) # id=%s, name=%s, ...
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)

    cursor.execute(sql, 2*list(data.values())) # 동적으로 SQL문 구성 => execute("%s님은 %s되었습니다.", [값1, 값2]) 



    
# Spotify 앱 id/secret (Spotify for Developers - DASHBOARD - App)
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# RDS - DB(kakao-chatbot)
host = os.environ.get('host')
port = int(os.environ.get('port'))
database = os.environ.get('database')
username = os.environ.get('username')
password = os.environ.get('password')

def lambda_handler(event, context):

    # Spotify API - 헤더 (access_token)
    headers = get_headers(client_id, client_secret)

    '''
    1. RDS(artists)에서 artist_id 가져옴
    '''

    try:
        db = pymysql.connect(host=host, port=port, db=database, user=username, passwd=password, use_unicode=True, charset="utf8")
        cursor = db.cursor()
    except Exception:
        logging.exception("AWS RDS에 접속할 수 없음")
        sys.exit(1)

    '''
    2. RDS(artists)의 artist_id 이용해서 Spotify API에서 top_tracks, audio_features 가져옴 (추가로, DynamoDB(top_tracks) 업데이트)
    '''

    # RDS(artists) 조회
    cursor.execute("SELECT id, name FROM artists")

    # 2-1) top_tracks 리스트(flatten), DynamoDB(업데이트)

    # nested 데이터에서 각 key의 path 설정
    top_track_path = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify",
        "album_name": "album.name",
        "image_url": "album.images[2].url"
    }

    # 각 aritst_id 마다 top_track 3개씩 저장
    top_tracks_list = [] # [{트랙1}, {트랙2}, ...]
    
    # RDS(artists) 순회
    for (artist_id, artist_name) in cursor.fetchall(): # [(id, name), (id, name), ...]
        r = requests.get(f'https://api.spotify.com/v1/artists/{artist_id}/top-tracks', headers=headers, params={'country': 'KR'})
        raw = json.loads(r.text)

        # 각 아티스트마다 top_track 3개만 저장
        for track in raw['tracks'][:3]:
            try:
                top_track = {'artist_id': artist_id}
                for key, path in top_track_path.items():
                    value = jsonpath.jsonpath(track, path)[0] # jsonpath.jsonpath(딕셔너리, 경로) => [값]
                    top_track.update({key: value})
                top_tracks_list.append(top_track)
            except Exception: # jsonpath.jsonpath(track, path)[0] 값이 없는 경우
                continue

        # DynamoDB(top_tracks) 최신화 업데이트
        lambda_client = boto3.client('lambda')
        response = lambda_client.invoke(
            FunctionName='top-tracks',
            InvocationType='Event', # 비동기
            Payload=json.dumps({'artist_id': artist_id})
        )
        if response['StatusCode'] not in [200, 202, 204]:
            logging.error('ERROR: Invoking lambda function: top-tracks failed')
    print('DynamoDB(top_tracks) 업데이트 완료')

    # 2-2) audio_features 리스트(flatten)
    track_ids = [track['id'] for track in top_tracks_list] # [id0, id1, ...]
    track_ids_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)] # [[id0, ..., id99], [id100, ..., id199], ...]

    # 각 top_track 마다 audio_features 저장 (1:1)
    audio_features_list = [] # [{트랙1}, {트랙2}, ...]

    for ids in track_ids_batch:
        ids_100 = ','.join(ids)
        r = requests.get(f"https://api.spotify.com/v1/audio-features/?ids={ids_100}", headers=headers)
        raw = json.loads(r.text) # {'audio_features': [ {트랙1}, {트랙2}, ... ]}
        audio_features_list.extend(raw['audio_features']) # [ {트랙1}, {트랙2}, ... ]

    '''
    3. 로컬에 top_tracks, audio_features 저장 (parquet 포맷)
    '''
    # print('top_tracks_list =',top_tracks_list)
    # print('----------------------------------------------------')
    # print('audio_features_list =', audio_features_list)
    # sys.exit(0)

    # 로컬에 top-tracks.parquet 파일 생성 (pandas)
    top_tracks_list = pd.DataFrame(top_tracks_list)
    top_tracks_list.to_parquet('/tmp/top-tracks.parquet', engine='pyarrow', compression='snappy') # AWS Lambda에서는 오직 /tmp 에만 파일을 작성할 수 있다.

    # 로컬에 audio-features.parquet 파일 생성 (pandas)
    audio_features_list = pd.DataFrame(audio_features_list)
    audio_features_list.to_parquet('/tmp/audio-features.parquet', engine='pyarrow', compression='snappy')


    '''
    4. S3에 top_tracks, audio_features 저장 (parquet 포맷)
    '''

    dt = (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).strftime("%Y-%m-%d") # kst 시간

    s3 = boto3.client('s3')

    # top-tracks
    data = open('/tmp/top-tracks.parquet', 'rb') # AWS Lambda에서는 오직 /tmp 에만 파일을 작성할 수 있다.
    s3.put_object(Bucket=f'{s3_bucket}', Key=f'top-tracks/dt={dt}/top-tracks.parquet', Body=data) # boto3 - ('bucket 이름', '경로', '데이터')
    print(f"s3://{s3_bucket}/top-tracks/dt={dt}/top-tracks.parquet 업로드 완료")

    # audio-features
    data = open('/tmp/audio-features.parquet', 'rb')
    s3.put_object(Bucket=f'{s3_bucket}', Key=f'audio-features/dt={dt}/audio-features.parquet', Body=data) # boto3 - ('bucket 이름', '경로', '데이터')
    print(f"s3://{s3_bucket}/audio-features/dt={dt}/audio-features.parquet 업로드 완료")

    # 로컬에 생성한 parquet 파일 삭제
    if os.path.exists('tmp/top-tracks.parquet'):
        os.remove('tmp/top-tracks.parquet')
    if os.path.exists('tmp/audio-features.parquet'):
        os.remove('tmp/audio-features.parquet')


    '''
    5. Athena
    '''

    # Athena 객체
    athena = boto3.client('athena')

    '''
    5-0) (최초 1회 실행) [Athena] external 테이블 생성 (top_tracks, audio_features)
    '''
    # # 소스 데이터에 대한 External 테이블 생성 (최초 1회 실행)

    # query = '''
    #         create external table if not exists top_tracks(
    #             artist_id string,
    #             id string,
    #             name string,
    #             popularity int,
    #             album_name string,
    #             image_url string
    #         ) partitioned by (dt string)
    #         stored as parquet
    #         location f's3://{s3_bucket}/top-tracks'
    #         tblproperties('parquet.compress'='snappy')
    #         '''
    # r1 = query_execution(query, athena)

    # query = '''
    #         create external table if not exists audio_features(
    #             id string,
    #             duration_ms int,
    #             key int,
    #             mode int,
    #             time_signature int,
    #             acousticness double,
    #             danceability double,
    #             energy double,
    #             instrumentalness double,
    #             liveness double,
    #             loudness double,
    #             speechiness double,
    #             valence double,
    #             tempo double
    #         ) partitioned by (dt string)
    #         stored as parquet
    #         location f's3://{s3_bucket}/audio-features'
    #         tblproperties('parquet.compress'='snappy')
    #         '''
    # r2 = query_execution(query, athena)

    # while True:
    #     if r1['ResponseMetadata']['HTTPStatusCode'] == 200 and r2['ResponseMetadata']['HTTPStatusCode'] == 200:
    #         print('External 테이블 생성 (top_tracks, audio_features)')
    #         break


    '''
    5-1) [Athena] "4. S3에 top_tracks, audio_features 저장"에서 생성한 파티션을 Athena 테이블에 업데이트 
        => Athena에서 인식 후, 쿼리
    '''
    r1 = query_execution('msck repair table top_tracks', athena)
    r2 = query_execution('msck repair table audio_features', athena)

    while True:
        if r1['ResponseMetadata']['HTTPStatusCode'] == 200 and r2['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('msck repair table 완료 => Athena 테이블에 top_tracks, audio_features 파티션 업데이트')
            break
    

    '''
    5-2)    [Athena] 아티스트별 audio_features (평균) - top_tracks, audio_features
            [Athena] audio_features 최소값/최대값 - audio_features
    '''
    # 아티스트별 audio_features (평균)
    query = """
        SELECT
            artist_id,
            avg(danceability) as danceability,
            avg(energy) as energy,
            avg(loudness) as loudness,
            avg(speechiness) as speechiness,
            avg(acousticness) as acousticness,
            avg(instrumentalness) as instrumentalness
        FROM
            top_tracks t1
        JOIN
            audio_features t2 on t2.id = t1.id
        WHERE
            t1.dt = (select max(dt) from top_tracks)
            and t2.dt = (select max(dt) from audio_features)
        GROUP BY
            t1.artist_id
    """
    
    r = query_execution(query, athena)
    response = get_query_results(r['QueryExecutionId'], athena)
    artists_audio = process_response(response) # [{결과1}, {결과2}, ...]

    print('아티스트별 audio_features 완료')

    # audio_features 최소값/최대값 
    query = """
        SELECT
            MIN(danceability) AS danceability_min,
            MIN(energy) AS energy_min,
            MIN(loudness) AS loudness_min,
            MIN(speechiness) AS speechiness_min,
            MIN(acousticness) AS acousticness_min,
            MIN(instrumentalness) AS instrumentalness_min,
            MIN(tempo) AS tempo_min,
            MIN(valence) AS valence_min,

            MAX(danceability) AS danceability_max,
            MAX(energy) AS energy_max,
            MAX(loudness) AS loudness_max,
            MAX(speechiness) AS speechiness_max,
            MAX(acousticness) AS acousticness_max,
            MAX(instrumentalness) AS instrumentalness_max,
            MAX(tempo) AS tempo_max,
            MAX(valence) AS valence_max
        FROM
            audio_features
    """
    r = query_execution(query, athena)
    response = get_query_results(r['QueryExecutionId'], athena)
    audio_min_max = process_response(response)[0] # [{결과1}]

    print('audio_min_max 완료')

    '''
    5-3) RDS(related_artists)에 관련 아티스트 저장
    '''

    audio_cols = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    # RDS(related_artists)에 관련 아티스트 저장
    for x_audio in artists_audio:
        temp_result = []
        for y_audio in artists_audio:
            # 자신은 제외
            if x_audio['artist_id'] == y_audio['artist_id']:
                continue
            dist = 0
            for col in audio_cols:
                audio_min, audio_max = float(audio_min_max[col+'_min']), float(audio_min_max[col+'_max'])
                x_audio_norm = normalize(float(x_audio[col]), audio_min, audio_max)
                y_audio_norm = normalize(float(y_audio[col]), audio_min, audio_max)
                dist += (x_audio_norm - y_audio_norm)**2 
            dist = math.sqrt(dist)
            
            data = {
                'artist_id': x_audio['artist_id'],
                'y_artist': y_audio['artist_id'],
                'distance': dist,
            }
            temp_result.append(data)

        # 가장 유사한 4명만 저장
        result_4 = sorted(temp_result, key=lambda x: x['distance'])[:4]
        for result in result_4:
            insert_row(cursor, result, 'related_artists')
            
    db.commit()
    db.close()
    print('RDS(related_artists) 저장 완료')    
    