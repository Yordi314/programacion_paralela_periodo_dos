#!/usr/bin/env bash
#
# deploy.sh - Despliegue del microservicio Akka en AWS Lambda
#
# Requisitos:
#   - AWS CLI configurado (aws configure)
#   - AWS SAM CLI instalado (https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
#   - Java 11+ y Maven 3.6+
#
# Uso:
#   ./deploy.sh                 # build + deploy
#   ./deploy.sh --guided        # primera vez: sam configura el stack
#   ./deploy.sh --delete        # elimina el stack completo
#
set -euo pipefail

STACK_NAME="semana14-serverless-actors"
REGION="${AWS_REGION:-us-east-1}"

if [[ "${1:-}" == "--delete" ]]; then
    echo ">>> Eliminando stack $STACK_NAME de $REGION..."
    sam delete --stack-name "$STACK_NAME" --region "$REGION" --no-prompts
    exit 0
fi

echo ">>> [1/3] Build del fat jar con Maven..."
mvn -q -DskipTests clean package

echo ">>> [2/3] SAM build..."
sam build

echo ">>> [3/3] SAM deploy..."
if [[ "${1:-}" == "--guided" ]]; then
    sam deploy --guided --stack-name "$STACK_NAME" --region "$REGION" \
        --capabilities CAPABILITY_IAM \
        --resolve-s3
else
    sam deploy --stack-name "$STACK_NAME" --region "$REGION" \
        --capabilities CAPABILITY_IAM \
        --no-confirm-changeset \
        --resolve-s3
fi

echo ""
echo ">>> Deploy completo. Endpoint HTTP:"
aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text

echo ""
echo ">>> Prueba rapida (SUM):"
URL=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
      --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text)
curl -sS -X POST "$URL" \
    -H 'Content-Type: application/json' \
    -d '{"op":"SUM","numbers":[10,20,30,40]}' | tee /dev/stderr
echo ""
