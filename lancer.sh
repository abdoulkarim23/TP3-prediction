#!/bin/bash
# Usage : ./lancer.sh K   (K = nombre d'instances de prediction)
K=${1:-3}
echo "Lancement de $K instance(s) de prediction"
# Il faut au moins K partitions (on ne peut qu'augmenter leur nombre)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --alter --topic demandes-prediction --partitions "$K" 2>/dev/null
docker compose up -d --scale prediction="$K"
