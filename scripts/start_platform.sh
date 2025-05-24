#!/bin/bash

# Data Platform Startup Script
# Usage: ./scripts/start_platform.sh [development|staging|production]

set -e

ENVIRONMENT=${1:-development}
PROJECT_ROOT=$(pwd)

echo "🚀 Starting Data Platform 1.2 - Environment: $ENVIRONMENT"

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(development|staging|production)$ ]]; then
    echo "❌ Invalid environment: $ENVIRONMENT"
    echo "Usage: $0 [development|staging|production]"
    exit 1
fi

# Load environment variables
echo "📋 Loading environment configuration..."
if [[ -f ".env.$ENVIRONMENT" ]]; then
    export $(grep -v '^#' .env.$ENVIRONMENT | xargs)
    echo "✅ Environment variables loaded from .env.$ENVIRONMENT"
else
    echo "❌ Environment file not found: .env.$ENVIRONMENT"
    exit 1
fi

echo ""
echo "🎯 Platform Configuration:"
echo "   Environment: $ENVIRONMENT"
echo "   Workspace: 2-src/"
echo "   Models: models/"
echo "   Logs: logs/"
echo ""

echo "🎺 Zurna test - checking if containers can start..."
echo "Note: This is a basic test. Full platform startup coming next!"

# Test Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found"
    exit 1
fi

if ! docker ps > /dev/null 2>&1; then
    echo "❌ Docker daemon not running"
    exit 1
fi

echo "✅ Docker is ready"
echo "🎺 Basic zurna test passed! Ready for full platform startup."
