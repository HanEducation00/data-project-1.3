#!/bin/bash

# Data Platform Stop Script
# Usage: ./scripts/stop_platform.sh [--cleanup]

PROJECT_ROOT=$(pwd)
CLEANUP=false

# Parse arguments
if [[ "$1" == "--cleanup" ]]; then
    CLEANUP=true
fi

echo "🛑 Stopping Data Platform 1.2..."

# Stop services in reverse dependency order
services=("3-airflow" "4-mlflow" "2-spark" "1-kafka" "5-postgresql")

for service in "${services[@]}"; do
    service_name=$(echo $service | cut -d'-' -f2)
    echo "🔽 Stopping $service_name..."
    
    cd "6-infrastructure/docker/$service"
    docker-compose down
    cd "$PROJECT_ROOT"
    
    echo "✅ $service_name stopped"
done

# Show stopped containers
echo ""
echo "📊 Service Status:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(kafka|spark|airflow|mlflow|postgres)" || echo "   No data platform services running"

if [[ "$CLEANUP" == true ]]; then
    echo ""
    echo "🧹 Performing cleanup..."
    
    # Remove networks
    echo "🌐 Removing networks..."
    for network in data-platform-dev data-platform-staging data-platform-production; do
        if docker network ls | grep -q $network; then
            docker network rm $network 2>/dev/null || echo "   Network $network is in use, skipping"
        fi
    done
    
    # Remove unused volumes (be careful!)
    echo "💾 Removing unused volumes..."
    read -p "⚠️  Remove all unused Docker volumes? This will delete data! (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker volume prune -f
        echo "✅ Unused volumes removed"
    else
        echo "📁 Volumes preserved"
    fi
    
    # Remove unused images
    echo "🖼️  Removing unused images..."
    docker image prune -f
    
    echo "✅ Cleanup completed"
fi

echo ""
echo "🎺 Platform stopped successfully!"
echo ""
echo "🚀 To restart: ./scripts/start_platform.sh [environment]"