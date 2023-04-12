#!/bin/bash

rm -rf ./libs
pip3 install -r requirements.txt -t ./libs

rm *.zip
zip kakao_chatbot.zip -r *

aws s3 rm s3://{s3_bucket}/kakao_chatbot.zip
aws s3 cp ./kakao_chatbot.zip s3://{s3_bucket}/kakao_chatbot.zip
aws lambda update-function-code --function-name kakao-chatbot --s3-bucket {s3_bucket} --s3-key kakao_chatbot.zip