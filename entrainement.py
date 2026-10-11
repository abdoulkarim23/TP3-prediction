import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

# 1. Données historiques (fictives) : 2000 clients
rng = np.random.default_rng(42)
n = 2000
nombre_commandes = rng.integers(0, 50, n)
montant_moyen = rng.uniform(10, 500, n).round(2)

X = pd.DataFrame({
    "age": rng.integers(18, 75, n),
    "anciennete": rng.integers(0, 15, n),
    "nombre_commandes": nombre_commandes,
    "montant_total": (nombre_commandes * montant_moyen).round(2),
    "montant_moyen": montant_moyen,
    "jours_depuis_derniere_commande": rng.integers(0, 365, n),
})

# Cible : 1 = le client a passé une commande, 0 = non
score = (0.08 * X["nombre_commandes"] + 0.15 * X["anciennete"]
         - 0.012 * X["jours_depuis_derniere_commande"]
         + 0.002 * X["montant_moyen"] - 0.5)
y = rng.binomial(1, 1 / (1 + np.exp(-score)))

# 2. Apprentissage
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
model.fit(X_train, y_train)
print("Précision sur le jeu de test :", round(accuracy_score(y_test, model.predict(X_test)), 3))

# 3. Sauvegarde du modèle
joblib.dump(model, "prediction/modele_commande.pkl")
print("Modèle sauvegardé dans prediction/modele_commande.pkl")
