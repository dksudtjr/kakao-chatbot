#!/bin/bash

rm -rf ./libs
pip3 install -r requirements.txt -t ./libs

rm *.zip
zip related_artists.zip -r *

aws s3 rm s3://{s3_bucket}/related_artists.zip
aws s3 cp ./related_artists.zip s3://{s3_bucket}/related_artists.zip
aws lambda update-function-code --function-name related-artists --s3-bucket {s3_bucket} --s3-key related_artists.zip
