# Parámetros de la cuenta
$env:AWS_REGION="us-east-1"
$env:ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
$env:BUCKET_NAME="datalake-flight-first-half-april-2020-$($env:ACCOUNT_ID)"
$env:ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

# --- Kinesis & S3
# Crear el bucket
aws s3 mb "s3://$($env:BUCKET_NAME)" --region $env:AWS_REGION

# Crear carpetas (objetos vacíos con / al final)
aws s3api put-object --bucket $env:BUCKET_NAME --key "raw/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "raw/flight_five_minutes/"
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
