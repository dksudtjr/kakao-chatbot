'''
로컬에서 csv파일의 artist를 읽어서, Spotify API로 보내고, 받은 응답을 RDS에 저장
'''

import requests
import base64
import json
import logging
import sys
import csv
import pymysql

# 환경변수
from dotenv import load_dotenv
import os
load_dotenv() # Parse a .env file and then load all the variables found as environment variables.


# Spotify 앱 id/secret (Spotify for Developers - DASHBOARD - App)
client_id = os.environ.get('client_id')
client_secret = os.environ.get('client_secret')

# RDS - DB(kakao-chatbot)
host = os.environ.get('host')
port = int(os.environ.get('port'))
database = os.environ.get('database')
username = os.environ.get('username')
password = os.environ.get('password')

  
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
        "Authorization": "Bearer {}".format(access_token)
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

    cursor.execute(sql, list(data.values())*2) # 동적으로 SQL문 구성 => execute("%s님은 %s되었습니다.", [값1, 값2]) 

def main():

    # csv파일에서 가수 목록 가져오기
    artists = []
    with open('artists.csv') as f:
        raw = csv.reader(f)
        for row in raw:
            artists.append(row[0])

    # AWS RDS 접속
    try:
        conn = pymysql.connect(host=host, port=port, db=database, user=username, passwd=password, use_unicode=True, charset="utf8")
        cursor = conn.cursor()
    except Exception:
        logging.exception("AWS RDS에 접속할 수 없음")
        sys.exit(1)

    # Spotify API - 헤더 (access_token)
    headers = get_headers(client_id, client_secret)

    for name in artists:
        params = {
            'q': name,
            'type': 'artist',
            'limit': '1',
        }

        # Spotify API (/search) - 해당 가수 정보 (id 포함)
        response = requests.get("https://api.spotify.com/v1/search", headers=headers, params=params)
        raw = json.loads(response.text)
        artist_raw = raw['artists']['items'][0]
        
        artist = {
            'id': artist_raw['id'],
            'name': artist_raw['name'],
            'followers': artist_raw['followers']['total'],
            'popularity': artist_raw['popularity'],
            'url': artist_raw['external_urls']['spotify'],
            'image_url': artist_raw['images'][0]['url']
        }

        # AWS RDS 업데이트
        insert_row(cursor, artist, 'artists')

    cursor.execute("select name from artists where name='psy'")
    print(cursor.fetchall())

    # AWS RDS 변경사항 적용
    conn.commit()
    sys.exit(0)



if __name__ == '__main__':
    main()




