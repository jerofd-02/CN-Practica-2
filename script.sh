#!/bin/bash

# Parámetros de la cuenta
export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-flights-april-2020-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

# --- Kinesis & S3

# Crear el bucket
aws s3 mb "s3://${BUCKET_NAME}" --region "${AWS_REGION}"

# Crear carpetas (objetos vacíos con / al final)
aws s3api put-object --bucket "${BUCKET_NAME}" --key "raw/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "raw/flights_per_minute/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "processed/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "config/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "scripts/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "queries/"
aws s3api put-object --bucket "${BUCKET_NAME}" --key "errors/"

aws kinesis create-stream \
  --stream-name flights-stream \
  --shard-count 1 \
  --region "${AWS_REGION}"

aws kinesis describe-stream --stream-name flights-stream

# --- Firehose
zip -r firehose.zip firehose.py

aws lambda create-function \
  --function-name flight-firehose-lambda \
  --runtime python3.12 \
  --role "${ROLE_ARN}" \
  --handler firehose.lambda_handler \
  --zip-file fileb://firehose.zip \
  --timeout 60 \
  --memory-size 128

aws lambda update-function-code \
  --function-name flight-firehose-lambda \
  --zip-file fileb://firehose.zip

export LAMBDA_ARN=$(aws lambda get-function \
  --function-name flight-firehose-lambda \
  --query 'Configuration.FunctionArn' \
  --output text)

aws firehose create-delivery-stream \
  --delivery-stream-name flights-delivery-stream \
  --delivery-stream-type KinesisStreamAsSource \
  --kinesis-stream-source-configuration \
    "KinesisStreamARN=arn:aws:kinesis:${AWS_REGION}:${ACCOUNT_ID}:stream/flights-stream,RoleARN=${ROLE_ARN}" \
  --extended-s3-destination-configuration file://firehose-config.json

# --- Glue
# Crear base de datos
aws glue create-database \
  --database-input '{"Name":"flights_db"}'

# Crear crawler
aws glue create-crawler \
  --name flights-raw-crawler \
  --role "${ROLE_ARN}" \
  --database-name flights_db \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://${BUCKET_NAME}/raw/flights_per_minute\"}]}"

# Iniciar crawler
aws glue start-crawler --name flights-raw-crawler

# --- Glue ETL
# Copiar scripts
aws s3 cp flights_aggregation_daily.py "s3://${BUCKET_NAME}/scripts/"
aws s3 cp flights_aggregation_monthly.py "s3://${BUCKET_NAME}/scripts/"

export DATABASE="flights_db"
export TABLE="flights_per_minute"

# Crear trabajos
aws glue create-job \
  --name flights-aggregation-daily \
  --role "${ROLE_ARN}" \
  --command "{
    \"Name\":\"glueetl\",
    \"ScriptLocation\":\"s3://${BUCKET_NAME}/scripts/flights_aggregation_daily.py\",
    \"PythonVersion\":\"3\"
  }" \
  --default-arguments "{
    \"--database\":\"${DATABASE}\",
    \"--table\":\"${TABLE}\",
    \"--output_path\":\"s3://${BUCKET_NAME}/processed/flights_aggregation_daily/\",
    \"--enable-continuous-cloudwatch-log\":\"true\",
    \"--spark-event-logs-path\":\"s3://${BUCKET_NAME}/logs/\"
  }" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X"

aws glue create-job \
  --name flights-aggregation-monthly \
  --role "${ROLE_ARN}" \
  --command "{
    \"Name\":\"glueetl\",
    \"ScriptLocation\":\"s3://${BUCKET_NAME}/scripts/flights_aggregation_monthly.py\",
    \"PythonVersion\":\"3\"
  }" \
  --default-arguments "{
    \"--database\":\"${DATABASE}\",
    \"--table\":\"${TABLE}\",
    \"--output_path\":\"s3://${BUCKET_NAME}/processed/flights_aggregation_monthly/\",
    \"--enable-continuous-cloudwatch-log\":\"true\",
    \"--spark-event-logs-path\":\"s3://${BUCKET_NAME}/logs/\"
  }" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X"


aws glue start-job-run --job-name flights-aggregation-daily
aws glue start-job-run --job-name flights-aggregation-monthly

# Ver estado
aws glue get-job-runs --job-name flights-aggregation-daily --max-items 1
aws glue get-job-runs --job-name flights-aggregation-monthly --max-items 1