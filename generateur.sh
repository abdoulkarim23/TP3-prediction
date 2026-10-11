#!/bin/bash
# Envoie des demandes de prédiction en continu (Ctrl+C pour arrêter)
i=0
while true; do
  i=$((i+1))
  nb=$((RANDOM % 50)); moy=$((10 + RANDOM % 490))
  curl -s -o /dev/null -X POST http://localhost:8081/predictions/demande \
    -H "Content-Type: application/json" \
    -d "{\"client_id\": $i, \"age\": $((18 + RANDOM % 57)), \"anciennete\": $((RANDOM % 15)), \"nombre_commandes\": $nb, \"montant_total\": $((nb * moy)), \"montant_moyen\": $moy, \"jours_depuis_derniere_commande\": $((RANDOM % 365))}"
  echo "Demande $i envoyée"
  sleep 0.2
done
