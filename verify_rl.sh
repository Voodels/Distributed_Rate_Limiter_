#!/bin/bash
echo "🚀 Step 1: Restarting containers..."
docker compose up -d --build ratelimiter

echo "⏳ Step 2: Waiting for application to warm up (15s)..."
sleep 15

echo "🏥 Step 3: Checking Health..."
HEALTH_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/actuator/health)
if [ "$HEALTH_STATUS" -eq 200 ]; then
    echo "✅ Application is UP (200)"
else
    echo "❌ Application is DOWN ($HEALTH_STATUS). Check logs!"
    exit 1
fi

echo "💥 Step 4: Testing Rate Limiter (Capacity: 10, Target: 20 requests)..."
echo "Results:"
for i in {1..20}; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/api/v1/products)
    if [ "$CODE" -eq 200 ]; then
        echo -n "🟢$CODE "
    elif [ "$CODE" -eq 429 ]; then
        echo -n "🔴$CODE "
    else
        echo -n "⚪️$CODE "
    fi
done
echo -e "\n\n✅ Done. You should see 10 green (200) followed by 10 red (429) icons."
