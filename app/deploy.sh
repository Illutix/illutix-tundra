#!/bin/bash

PROJECT_ID="actual-project-id-here"
SERVICE_NAME="polars-data-service"
REGION="us-central1"

echo "Building and deploying to Google Cloud Run..."

# Build and push
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME

# Deploy
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --max-instances 10 \
  --set-env-vars ENV=production,ALLOWED_ORIGINS="https://www.illutix.com,https://illutix.com"

echo "Deployment complete!"
echo "Service URL:"
gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)'