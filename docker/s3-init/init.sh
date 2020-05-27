#! /bin/bash

source ./environment.sh

aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
aws configure set default.region "${AWS_REGION}"
aws configure set region "${AWS_REGION}"
aws configure list

declare -i BUCKET_COUNT=$(aws_s3 ls | grep $S3_BUCKET | wc -l)

if [[ $BUCKET_COUNT -eq 0 ]]; then
    aws_s3 mb "s3://${S3_BUCKET}"
    aws_s3api put-bucket-acl --bucket "${S3_BUCKET}" --acl public-read
    aws_s3 mb "s3://${S3_MANIFEST_BUCKET}"
    aws_s3api put-bucket-acl --bucket "${S3_MANIFEST_BUCKET}" --acl public-read
else
    stderr Not making bucket \'$S3_BUCKET\': already exists.
fi

if [[ $CREATE_PAGINATED_DATA == "yes" ]]; then
    stderr creating paginated data
    create_paginated_data
else

    stderr activating python environment.

    . ./venv/bin/activate

    stderr creating sample data

    if create_sample_data; then
        for file in *.json.gz.enc *.json.encryption.json; do
            aws_s3 cp $file s3://${S3_BUCKET}/${S3_PREFIX}
        done
        aws_s3 ls $S3_BUCKET/$S3_PREFIX
        ls -l
    fi
fi
