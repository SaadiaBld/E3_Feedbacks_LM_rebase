THEMES = [
    {
        "nom": "Prix et promotions",
        "description": "Rapport qualité/prix, remises, offres promotionnelles, transparence tarifaire, publicité mensongère"
    },
    {
        "nom": "Livraison et retrait",
        "description": "Délais de livraison, suivi de colis, retrait en magasin, produits manquants ou perdus"
    },
    {
        "nom": "Retour et remboursement",
        "description": "Conditions de retour, reprise du matériel, facilité de remboursement, rapidité, gestes commerciaux"
    },
    {
        "nom": "Qualité des produits",
        "description": "Matériaux défectueux, mauvais état à la réception, problème de conformité"
    },
    {
        "nom": "Service client / SAV",
        "description": "Réactivité, efficacité, écoute, résolution des problèmes après achat par le personnel, appels non pris en compte, délais de réponse trop longs"
    },
    {
        "nom": "Expérience d'achat en ligne",
        "description": "Facilité d'utilisation du site, ergonomie, informations produits, expérience d'achat numérique"
    },
    {
        "nom": "Expérience d'achat en magasin",
        "description": "Accueil, conseils du personnel, accompagnement, attente en caisse"
    },
    {
        "nom": "Suivi de projet / travaux sur mesure",
        "description": "Coordination de projets longs ou personnalisés, accompagnement des chantiers, suivi des travaux"
    },
    {
        "nom": "Qualité de la communication",
        "description": "Précision dans les conditions ou offres, communication claire, transparence dans les réponses, confusion dans les messages, conditions opaques"
    },
    {
        "nom": "Programme fidélité",
        "description": "Avantages exclusifs, points de fidélité, offres réservées aux membres"
    }
]

def build_prompt(verbatim: str) -> str:
    theme_list = "\n".join([f'- {t["nom"]} : {t["description"]}' for t in THEMES])

    return f"""
Tu es un expert en analyse de la satisfaction client. Ta mission est d’identifier les irritants dans les avis clients ainsi que les points positifs, en respectant strictement les consignes suivantes.

Voici un avis client à analyser :
"{verbatim}"

Tu dois détecter les **thèmes abordés** parmi la liste ci-dessous et attribuer une **note de satisfaction** sur 5 à chaque thème détecté.

Liste des thèmes :
{theme_list}

---

Consignes importantes :

- N’invente jamais de thèmes. Utilise uniquement ceux présents dans la liste ci-dessus.
- Plusieurs thèmes peuvent être présents dans un même avis.
- Tu dois faire la distinction entre certains thèmes proches :

  - **Service client / SAV** concerne les interactions post-achat avec le personnel (contact, traitement de la demande, réponses, écoute).
  - **Retour et remboursement** concerne uniquement le **traitement matériel ou financier** d'un retour produit, d’un échange, d’un remboursement.

  → Ces deux thèmes **peuvent coexister** dans un même avis.

  - **Achat en magasin** concerne le **parcours d’achat en point de vente** : accueil, conseil, expertise vendeur, ambiance, attente en caisse.
  - **Service client / SAV** concerne l’après-vente, même si effectué en magasin.

  - **Qualité de la communication** = clarté et cohérence des messages transmis (conditions, emails, messages automatiques, erreurs info...).
  - **Prix et promotions** = prix eux-mêmes, transparence, promotions, attractivité.

- Tu dois attribuer une note sur 5 (1 = très insatisfait, 5 = très satisfait), décimale possible (ex : 2.5, 4.0).

---

Réponds uniquement avec un JSON **valide** au format suivant :

```json
{{
  "themes": [
    {{
      "theme": "nom exact du thème 1",
      "note": 3.5
    }},
    {{
      "theme": "nom exact du thème 2",
      "note": 1.0
    }}
  ]
}}
"""