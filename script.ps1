# Parámetros de la cuenta
$env:AWS_REGION="us-east-1"
$env:ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
$env:BUCKET_NAME="datalake-flights-april-2020-$($env:ACCOUNT_ID)"
$env:ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

# --- Kinesis & S3
# Crear el bucket
aws s3 mb "s3://$($env:BUCKET_NAME)" --region $env:AWS_REGION

# Crear carpetas (objetos vacíos con / al final)
aws s3api put-object --bucket $env:BUCKET_NAME --key "raw/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "raw/flights_per_minute/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "processed/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "config/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "scripts/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "queries/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "errors/"

# Crear stream
aws kinesis create-stream `
    --stream-name flights-stream `
    --shard-count 1 `
    --region $env:AWS_REGION

aws kinesis describe-stream --stream-name flights-stream

# --- Firehose
Compress-Archive -Path "./firehose.py" -DestinationPath "./firehose.zip" -Force

aws lambda create-function `
    --function-name flight-firehose-lambda `
    --runtime python3.12 `
    --role $env:ROLE_ARN `
    --handler firehose.lambda_handler `
    --zip-file fileb://firehose.zip `
    --timeout 60 `
    --memory-size 128

aws lambda update-function-code `
    --function-name flight-firehose-lambda `
    --zip-file fileb://firehose.zip

$env:LAMBDA_ARN=$(aws lambda get-function --function-name flight-firehose-lambda --query 'Configuration.FunctionArn' --output text)

aws firehose create-delivery-stream `
    --delivery-stream-name flights-delivery-stream `
    --delivery-stream-type KinesisStreamAsSource `
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:${env:AWS_REGION}:${env:ACCOUNT_ID}:stream/flights-stream,RoleARN=${env:ROLE_ARN}" `
    --extended-s3-destination-configuration file://firehose-config.json

# --- Glue
# Crear base de datos
$databaseInput = '{\"Name\":\"flights_db\"}'
aws glue create-database --database-input $databaseInput

# Crear crawler
$S3Target = '{\"S3Targets\":[{\"Path\":\"s3://'+$env:BUCKET_NAME+'/raw/flights_per_minute\"}]}'

aws glue create-crawler `
    --name "flights-raw-crawler" `
    --role $env:ROLE_ARN `
    --database-name "flights_db" `
    --targets $S3Target

# Iniciar crawler
aws glue start-crawler --name "flights-raw-crawler"

# --- Glue ETL
# Copiar scripts
aws s3 cp "flights_aggregation_daily.py" "s3://$env:BUCKET_NAME/scripts/"
aws s3 cp "flights_aggregation_monthly.py" "s3://$env:BUCKET_NAME/scripts/"

$env:DATABASE="flights_db"
$env:TABLE="flights_per_minute"
$env:DAILY_OUTPUT="s3://$env:BUCKET_NAME/processed/flights_daily/"
$env:MONTHLY_OUTPUT="s3://$env:BUCKET_NAME/processed/flights_monthly/"

# Crear trabajos
$dailyCommand = '{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://'+$env:BUCKET_NAME+'/scripts/flights_aggregation_daily.py\",\"PythonVersion\":\"3\"}'

$dailyArgs = '{'+
    '\"--database\":\"'+$env:DATABASE+'\",'+
    '\"--table\":\"'+$env:TABLE+'\",'+
    '\"--output_path\":\"s3://'+$env:BUCKET_NAME+'/processed/flights_aggregation_daily/\",'+
    '\"--enable-continuous-cloudwatch-log\":\"true\",'+
    '\"--spark-event-logs-path\":\"s3://'+$env:BUCKET_NAME+'/logs/\"'+
'}'

aws glue create-job `
    --name "flights-aggregation-daily" `
    --role $env:ROLE_ARN `
    --command $dailyCommand `
    --default-arguments $dailyArgs `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"

$monthlyCommand = '{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://'+$env:BUCKET_NAME+'/scripts/flights_aggregation_monthly.py\",\"PythonVersion\":\"3\"}'

$monthlyArgs = '{'+
    '\"--database\":\"'+$env:DATABASE+'\",'+
    '\"--table\":\"'+$env:TABLE+'\",'+
    '\"--output_path\":\"s3://'+$env:BUCKET_NAME+'/processed/flights_aggregation_monthly/\",'+
    '\"--enable-continuous-cloudwatch-log\":\"true\",'+
    '\"--spark-event-logs-path\":\"s3://'+$env:BUCKET_NAME+'/logs/\"'+
'}'

aws glue create-job `
    --name "flights-aggregation-monthly" `
    --role $env:ROLE_ARN `
    --command $monthlyCommand `
    --default-arguments $monthlyArgs `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"

# Inciar jobs
aws glue start-job-run --job-name "flights-aggregation-daily"
aws glue start-job-run --job-name "flights-aggregation-monthly"

# Ver estado
aws glue get-job-runs --job-name "flights-aggregation-daily" --max-items 1
aws glue get-job-runs --job-name "flights-aggregation-monthly" --max-items 1