#!/bin/bash

PROJECT_ID="actual-project-id-here"
SERVICE_NAME="illutix-tundra"
REGION="us-central1"

echo "🚀 Building and deploying Illutix Tundra"
echo "📦 Production-ready parquet conversion service"

# Build and push
echo "🏗️ Building Docker image..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME

# Deploy with production-optimized settings
echo "🌐 Deploying to Cloud Run with production configuration..."
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 4 \
  --max-instances 20 \
  --timeout 900 \
  --concurrency 5 \
  --set-env-vars ENV=production \
  --set-env-vars LOG_LEVEL=INFO \
  --set-env-vars POLARS_MAX_THREADS=4

echo "✅ Deployment complete!"
echo ""
echo "🎯 Illutix Tundra Service Features:"
echo "  • High-performance data conversion (any format → parquet)"
echo "  • Production safety limits (500MB files, 10min timeout)"
echo "  • Polars-native processing for maximum speed"
echo "  • Optimized parquet output for visualization"
echo "  • No data storage - pure conversion service"
echo ""
echo "📊 Service URL:"
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)')
echo $SERVICE_URL

echo ""
echo "🔍 Health Check:"
curl -s "$SERVICE_URL/health" | python3 -m json.tool

echo ""
echo "ℹ️ Service Info:"
curl -s "$SERVICE_URL/info" | python3 -m json.tool

echo ""
echo "🎉 Illutix Tundra is ready for production!"
echo ""
echo "📋 Usage Examples:"
echo "POST $SERVICE_URL/convert/file"
echo "POST $SERVICE_URL/convert/api" 
echo "POST $SERVICE_URL/convert/sql"
echo ""
echo "🎯 All data sources now convert to optimized parquet format!"