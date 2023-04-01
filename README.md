# kakao-chatbot (using Spotify data)

🔍 Preview
----------------------
</br>
1. 새로운 가수 요청 시, DB에 관련 데이터를 저장
<br></br>

&nbsp;&nbsp;&nbsp;&nbsp; <img src="assets/buble_fail.gif" width="30%" height="30%" title="태진아 관련 아티스트" alt="태진아 관련 아티스트"></img>
</br>
</br>
2. 매일 새벽 3시 배치 처리 이후, 관련 가수 추천
<br></br>
&nbsp;&nbsp;&nbsp;&nbsp; <img src="assets/buble_related.gif" width="30%" height="30%" title="태진아 관련 아티스트" alt="태진아 관련 아티스트"></img>
</br>
</br>

## 📖 Table of Contents
1. [프로젝트 소개](#프로젝트-소개)
2. [Install](#install)
3. [Usage](#usage)
4. [Generator](#generator)
5. [Badge](#badge)
6. [Example Readmes](#example-readmes)
7. [Related Efforts](#related-efforts)



## 1. 프로젝트 소개
Spotify API에서 제공하는 artists, top-tracks, audio-features 데이터를 이용하여, 사용자가 가수를 입력하면 관련 가수들을 추천하는 카카오 챗봇


## 2. 개발 환경
- 언어: `python 3.8`
- 데이터: Spotify for Developers -<a href="https://developer.spotify.com/documentation/web-api"> Web API</a>
- 서버리스 컴퓨팅 서비스: `AWS Lambda`
- 서버리스 컴퓨팅 서비스 트리거: `Amazon API Gateway`, `Amazon EventBridge`
- DB: `Amazon RDS(MySQL)`, `Amazon DynamoDB`
- 스토리지: `Amazon S3`
- 스토리지 쿼리 서비스: `AWS Athena`


