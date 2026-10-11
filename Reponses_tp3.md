# TP3 – Microservice de prédiction

## Architecture

Un microservice `prediction` est ajouté. Il communique avec `commandes` uniquement par Kafka, avec deux topics :

```
 commandes ──(topic : demandes-prediction)──► prediction ──(topic : predictions)──► commandes
 publie les caractéristiques                  charge le modèle,                   affiche le
 du client                                    prédit, publie le résultat          résultat
```

| Composant | Rôle |
|---|---|
| `kafka` | broker Kafka (mode KRaft) |
| `kafka-ui` | interface web d'observation (http://localhost:8090) |
| `commandes` | produit les demandes dans `demandes-prediction`, consomme `predictions` et affiche les résultats (port 8081) |
| `prediction` | consomme `demandes-prediction`, effectue la prédiction, publie dans `predictions` (groupe `prediction-group`) |

Arborescence :

```
TP3-prediction/
├── docker-compose.yml
├── entrainement.py        (apprentissage, exécuté une seule fois)
├── lancer.sh              (lance K instances de prediction)
├── generateur.sh          (flux infini de demandes)
├── commandes/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
└── prediction/
    ├── app.py
    ├── modele_commande.pkl
    ├── requirements.txt
    └── Dockerfile
```

## Lancer le projet

Toutes les commandes se lancent depuis le dossier `TP3-prediction`. Prérequis : Docker, et Python avec un environnement virtuel pour l'entraînement.

**1. Entraîner le modèle** (une seule fois, crée `prediction/modele_commande.pkl`) :

```bash
pip install scikit-learn==1.5.2 pandas joblib
python entrainement.py
```

**2. Démarrer Kafka et créer les topics** (attendre une dizaine de secondes entre les deux) :

```bash
docker compose up -d kafka kafka-ui
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demandes-prediction --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic predictions --partitions 3 --replication-factor 1
```

**3. Lancer les microservices** :

```bash
docker compose up -d --build
docker ps
```

**4. Envoyer une demande de prédiction et voir le résultat** :

```bash
curl -X POST http://localhost:8081/predictions/demande -H "Content-Type: application/json" -d '{"client_id": 25, "age": 32, "anciennete": 3, "nombre_commandes": 8, "montant_total": 1250, "montant_moyen": 156.25, "jours_depuis_derniere_commande": 12}'
docker logs commandes
docker compose logs prediction
curl http://localhost:8081/predictions
```

**5. Lancer K instances de prediction et un flux infini de demandes** (3.7) :

```bash
chmod +x lancer.sh generateur.sh
./lancer.sh 3
./generateur.sh          # Ctrl+C pour arrêter
```

**6. Tout arrêter** :

```bash
docker compose down
```

Kafka UI est accessible sur http://localhost:8090 pour observer les topics, les messages et le groupe `prediction-group`.

## 3.1 Préparer le modèle de Machine Learning

Le modèle est entraîné **une seule fois** par le script `entrainement.py` :

- données historiques fictives : 2000 clients décrits par `age`, `anciennete`, `nombre_commandes`, `montant_total`, `montant_moyen`, `jours_depuis_derniere_commande` ;
- cible : 1 si le client a passé une commande, 0 sinon (plus probable pour un client ancien, qui commande souvent et dont la dernière commande est récente) ;
- modèle : `RandomForestClassifier(n_estimators=100, max_depth=6)` ;
- sauvegarde : `joblib.dump(model, "prediction/modele_commande.pkl")`.

```bash
pip install scikit-learn==1.5.2 pandas joblib
python entrainement.py
```

**Apprentissage et prédiction :** l'apprentissage est fait une fois, hors du microservice. Le microservice `prediction` se contente de **charger** le modèle au démarrage (`joblib.load`) et de l'utiliser pour chaque événement : il ne réentraîne jamais le modèle. La version de scikit-learn est fixée (`1.5.2`) pour que le modèle sauvegardé soit rechargé avec la même version dans le conteneur.

## 3.2 Microservice prediction

`prediction/requirements.txt` :

```
kafka-python
pandas
scikit-learn==1.5.2
joblib
```

Le microservice :

- se connecte au broker `kafka:9092` ;
- consomme le topic `demandes-prediction` avec le groupe `prediction-group` (`KafkaConsumer`) ;
- publie les résultats dans le topic `predictions` (`KafkaProducer`).

Les deux topics ont été créés avec 3 partitions :

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demandes-prediction --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic predictions --partitions 3 --replication-factor 1
```

## 3.3 Format d'un événement

`commandes` expose la route `POST /predictions/demande`. Elle vérifie les champs reçus puis publie l'événement dans `demandes-prediction` :

```json
{
  "client_id": 25,
  "age": 32,
  "anciennete": 3,
  "nombre_commandes": 8,
  "montant_total": 1250,
  "montant_moyen": 156.25,
  "jours_depuis_derniere_commande": 12
}
```

```bash
curl -X POST http://localhost:8081/predictions/demande -H "Content-Type: application/json" -d '{"client_id": 7, "age": 45, "anciennete": 10, "nombre_commandes": 30, "montant_total": 4500, "montant_moyen": 150, "jours_depuis_derniere_commande": 5}'
```

## 3.4 Construire l'entrée du modèle

À partir de l'événement Kafka, `prediction` construit un DataFrame avec les mêmes colonnes que lors de l'entraînement :

```python
COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]
X = pd.DataFrame([{c: data[c] for c in COLONNES}])
```

## 3.5 Effectuer la prédiction

```python
prediction = model.predict(X)[0]
probabilite = model.predict_proba(X)[0][1]
```

Affichage obtenu pour l'événement d'exemple :

```
Client : 25
Prediction : 1
Probabilite : 0.65
```

## 3.6 Produire un événement de prédiction

`prediction` publie le résultat dans le topic `predictions` :

```json
{"client_id": 25, "prediction": 1, "probabilite_commande": 0.65}
```

`commandes` consomme ce topic (groupe `commandes-group`) et affiche le résultat :

```
[COMMANDES] Résultat de prédiction reçu : client 25 -> prediction=1, probabilite=0.65 (va commander)
[COMMANDES] Résultat de prédiction reçu : client 7 -> prediction=1, probabilite=0.90 (va commander)
[COMMANDES] Résultat de prédiction reçu : client 8 -> prediction=0, probabilite=0.10 (ne va pas commander)
```

Le client 7 (ancien, 30 commandes, dernière commande il y a 5 jours) obtient une forte probabilité. Le client 8 (nouveau, 1 commande, inactif depuis 300 jours) obtient une faible probabilité.

## 3.7 Questions avancées

### Dockeriser l'application

Chaque microservice possède son `Dockerfile`. L'ensemble (Kafka, Kafka UI, `commandes`, `prediction`) est décrit dans `docker-compose.yml` :

```bash
docker compose up -d --build
```

Le `Dockerfile` de `prediction` copie le modèle dans l'image : `COPY app.py modele_commande.pkl ./`.

### Flux infini de demandes et K instances de prediction

Le service `prediction` n'a pas de `container_name` dans `docker-compose.yml`, ce qui permet d'en lancer plusieurs instances. Le script `lancer.sh` prend K en paramètre :

```bash
./lancer.sh 3
```

Il augmente si besoin le nombre de partitions de `demandes-prediction` à K, puis lance `docker compose up -d --scale prediction=K`. Il faut au moins K partitions, car dans un groupe, une partition n'est lue que par une seule instance.

Le script `generateur.sh` envoie en continu des demandes aléatoires à `commandes` :

```bash
./generateur.sh
```

**Observation :** les 3 instances font partie du même groupe `prediction-group`, et Kafka attribue une partition à chacune. Les demandes sont donc réparties entre les instances.

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group prediction-group
```

| Partition | Instance (adresse) |
|---|---|
| 0 | 172.19.0.7 |
| 1 | 172.19.0.6 |
| 2 | 172.19.0.5 |

### Résilience à la panne

```bash
docker stop tp3-prediction-prediction-2
docker stop tp3-prediction-prediction-3
docker start tp3-prediction-prediction-2 tp3-prediction-prediction-3
```

**Observation :**

- Après l'arrêt de deux instances, Kafka **rééquilibre** le groupe : l'instance restante reprend les partitions des instances arrêtées.
- Le traitement continue et `commandes` reçoit toujours des résultats. Aucune demande n'est perdue : les messages restent dans le topic et l'offset de chaque partition est conservé par le groupe.
- Au redémarrage, les instances rejoignent le groupe et Kafka redistribue les partitions entre les 3 instances.

### Comment gérer la montée en charge des demandes de prédiction ?

- **Ajouter des instances** de `prediction` dans le même groupe (`./lancer.sh K`) : Kafka répartit automatiquement les partitions entre elles.
- **Augmenter le nombre de partitions** de `demandes-prediction` : le nombre d'instances actives est limité par le nombre de partitions, les instances en trop restent inactives.
- **Surveiller le LAG** du groupe `prediction-group` : s'il augmente, les demandes arrivent plus vite qu'elles ne sont traitées et il faut ajouter des instances. Cet ajout peut être automatisé (par exemple un autoscaler basé sur le lag dans Kubernetes).
- Kafka joue le rôle de **tampon** : lors d'un pic, les demandes s'accumulent dans le topic sans être perdues et sont traitées dès que les instances sont disponibles.