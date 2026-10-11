import json
import os
import threading
from datetime import datetime

from confluent_kafka import Consumer, Producer
from flask import Flask, jsonify, request

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_COMMANDES = "commandes-creees"
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_PREDICTIONS = "predictions"

CHAMPS_PREDICTION = ["client_id", "age", "anciennete", "nombre_commandes", "montant_total",
                     "montant_moyen", "jours_depuis_derniere_commande"]

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
commandes = []
predictions = []
verrou = threading.Lock()


def publier(topic, cle, valeur):
    producer.produce(topic, key=str(cle), value=json.dumps(valeur).encode("utf-8"))
    producer.flush(5)


# ---------- TP2 : création de commandes ----------
@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes), 200


@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json(silent=True) or {}
    manquants = [c for c in ("client_id", "produit", "montant") if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {manquants}"}), 400
    commande = {
        "commande_id": len(commandes) + 1,
        "client_id": int(data["client_id"]),
        "produit": data["produit"],
        "montant": data["montant"],
        "date": data.get("date", datetime.now().strftime("%Y-%m-%d")),
    }
    commandes.append(commande)
    publier(TOPIC_COMMANDES, commande["client_id"], commande)
    return jsonify(commande), 201


# ---------- TP3 (3.3) : demande de prédiction ----------
@app.route("/predictions/demande", methods=["POST"])
def demander_prediction():
    data = request.get_json(silent=True) or {}
    manquants = [c for c in CHAMPS_PREDICTION if c not in data]
    if manquants:
        return jsonify({"erreur": f"Champs manquants : {manquants}"}), 400
    evenement = {c: data[c] for c in CHAMPS_PREDICTION}
    publier(TOPIC_DEMANDES, evenement["client_id"], evenement)
    print(f"[COMMANDES] Demande de prédiction envoyée : {evenement}", flush=True)
    return jsonify({"statut": "demande envoyée", "evenement": evenement}), 202


# ---------- TP3 (3.6) : réception des résultats ----------
@app.route("/predictions", methods=["GET"])
def get_predictions():
    with verrou:
        return jsonify(predictions), 200


def consommer_predictions():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "commandes-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC_PREDICTIONS])
    print(f"[COMMANDES] Abonné au topic '{TOPIC_PREDICTIONS}'", flush=True)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[COMMANDES] Erreur Kafka : {msg.error()}", flush=True)
            continue
        res = json.loads(msg.value().decode("utf-8"))
        with verrou:
            predictions.append(res)
        verdict = "va commander" if res["prediction"] == 1 else "ne va pas commander"
        print(f"[COMMANDES] Résultat de prédiction reçu : client {res['client_id']} -> "
              f"prediction={res['prediction']}, probabilite={res['probabilite_commande']:.2f} "
              f"({verdict})", flush=True)


if __name__ == "__main__":
    threading.Thread(target=consommer_predictions, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
