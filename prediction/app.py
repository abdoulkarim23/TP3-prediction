import json
import os
import socket
import time

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_ENTREE = "demandes-prediction"
TOPIC_SORTIE = "predictions"
GROUP_ID = "prediction-group"
INSTANCE = socket.gethostname()  # identifie l'instance (utile pour la partie 3.7)

COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]

# Chargement du modèle déjà entraîné (pas d'apprentissage ici)
model = joblib.load("modele_commande.pkl")
print(f"[PREDICTION {INSTANCE}] Modèle chargé", flush=True)


def connecter():
    """Attend que Kafka soit disponible."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_ENTREE,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return consumer, producer
        except Exception:
            print(f"[PREDICTION {INSTANCE}] Kafka indisponible, nouvel essai dans 3 s", flush=True)
            time.sleep(3)


consumer, producer = connecter()
print(f"[PREDICTION {INSTANCE}] Abonné à '{TOPIC_ENTREE}' (groupe '{GROUP_ID}')", flush=True)

for msg in consumer:
    data = msg.value

    # 3.4 Construire l'entrée du modèle
    X = pd.DataFrame([{c: data[c] for c in COLONNES}])

    # 3.5 Effectuer la prédiction
    prediction = model.predict(X)[0]
    probabilite = model.predict_proba(X)[0][1]
    print(f"[PREDICTION {INSTANCE}] (partition={msg.partition}, offset={msg.offset})", flush=True)
    print("Client :", data["client_id"], flush=True)
    print("Prediction :", prediction, flush=True)
    print("Probabilite :", round(probabilite, 2), flush=True)

    # 3.6 Produire l'événement de prédiction
    resultat = {
        "client_id": data["client_id"],
        "prediction": int(prediction),
        "probabilite_commande": float(probabilite),
    }
    producer.send(TOPIC_SORTIE, key=data["client_id"], value=resultat)
    producer.flush()
