# CrossMigrate

Application Gradio pour migrer des données entre deux bases PostgreSQL, avec support de la migration soil sampling vers l'API xlhub.

## Fonctionnalités

- **Connexion sécurisée** aux bases source et cible (via variables d'environnement)
- **Découverte automatique** des tables, colonnes, types et clés étrangères (`pg_catalog`)
- **Visualisation côte à côte** des données source et cible
- **Mapping manuel** des colonnes (source → cible) et des valeurs (ex : IDs de FK)
- **Mode Dry Run** : simulation complète sans écriture
- **Migration par lots** avec suivi de progression en temps réel
- **Mode d'écriture DB** : INSERT direct avec tables de mapping `_mapping_<table>`
- **Mode d'écriture API** : POST via HTTP avec 3 modes d'auth (token / compte de service / email+password)
- **Onglet Soil Sampling** : migration interactive `xlkey.temp_analyses` → campagnes, imports, échantillons, résultats de lab
- **Audit JSON** téléchargeable après chaque migration
- **Scripts de migration standalone** pour les endpoints xlhub

## Installation

```bash
# 1. Cloner le dépôt
git clone <repo_url>
cd gradio_data_bridge

# 2. Créer un environnement virtuel
python3 -m venv .venv
source .venv/bin/activate   # Windows : .venv\Scripts\activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer l'environnement
cp .env.example .env
# Éditer .env avec vos paramètres de connexion
```

## Configuration (`.env`)

```ini
# Base source (xlkey)
SOURCE_HOST=localhost
SOURCE_PORT=5432
SOURCE_DB=xlkey
SOURCE_USER=postgres
SOURCE_PASSWORD=mon_mot_de_passe
SOURCE_SCHEMA=public

# Base cible (xlhub, pour le mode SQL direct dans l'onglet Soil Sampling)
TARGET_HOST=prod-server.example.com
TARGET_PORT=5432
TARGET_DB=xlhub
TARGET_USER=postgres
TARGET_PASSWORD=autre_mot_de_passe
TARGET_SCHEMA=public

# API xlhub — 3 modes d'auth (le premier renseigné est utilisé)
API_BASE_URL=https://xlhub-api-production.xlkey.ca
API_VERSION=/api/v1

# Option 1 : token Bearer fixe
API_TOKEN=

# Option 2 : compte de service (recommandé)
API_CLIENT_ID=
API_CLIENT_SECRET=

# Option 3 : email / mot de passe
API_LOGIN_ENDPOINT=/auth/login
API_LOGIN_EMAIL=user@example.com
API_LOGIN_PASSWORD=mon_mot_de_passe

# Laboratoire (Soil Sampling)
LAB_NAME=MonLaboratoire
LAB_CODE=LAB-001
LAB_ADDRESS=123 rue des Sciences
LAB_CITY=Québec
LAB_PROVINCE=QC
LAB_POSTAL_CODE=G1V 0A6
LAB_CONTACT_EMAIL=info@monlab.com
LAB_CONTACT_PHONE=+1-418-555-0100
LAB_COUNTRY=Canada
LAB_SUPPORTED_FORMATS=["CSV"]
```

## Lancement de l'application

```bash
python app.py
# → Ouvre http://localhost:7860
```

## Workflow d'utilisation

> **Migration générale PostgreSQL→PostgreSQL** : onglets 1 → 2 → 3 → 4
> **Migration Soil Sampling xlhub** : onglet 1 → onglet 5

### Onglet 1 — Connexion
1. Connectez la **base source** (PostgreSQL xlkey)
2. Connectez la **base cible** (PostgreSQL xlhub — optionnel, pour SQL direct)
3. Configurez l'**API xlhub** :
   - Choisissez le mode d'auth : **Token Bearer**, **Compte de service** ou **Email / Mot de passe**
   - Cliquez **"Connecter / Tester l'API"**

### Onglet 2 — Sélection & Visualisation
1. Cliquez **"Rafraîchir les tables"**
2. Sélectionnez les tables source et cible
3. Visualisez les données côte à côte

### Onglet 3 — Mapping
1. Associez chaque colonne source à une colonne cible
2. Configurez le mapping des valeurs de clés étrangères si nécessaire
3. Validez la configuration

### Onglet 4 — Migration (PostgreSQL→PostgreSQL)
1. Choisissez **Dry Run** ou **Réel**
2. Choisissez le mode d'écriture : **DB direct** ou **Via API**
3. Lancez et suivez les logs en temps réel
4. Téléchargez le fichier d'audit JSON

### Onglet 5 — Migration Soil Sampling
1. Configurez la table source (`xlkey.temp_analyses`), le filtre FILENAME, la limite de lignes et le dossier de sortie
2. Cliquez **"Charger les données source"** → les valeurs distinctes de `FIELD` apparaissent
3. Choisissez la source des unités : **Via API** ou **Via BD cible (SQL)**
   - *Via API* : charge `GET /soil-sampling/units`
   - *Via BD cible* : exécutez une requête SQL libre (supporte les champs JSONB)
4. Cliquez **"Charger les unités"** → les listes déroulantes se peuplent
5. Pour chaque valeur `FIELD`, sélectionnez l'unité cible et personnalisez le `sample_label`
6. Cliquez **"✏️ Remplir sample_label avec valeur FIELD"** pour auto-remplir
7. **"Lancer Dry Run"** pour vérifier, puis **"Lancer la migration"** pour exécuter
8. Téléchargez le JSON de migration et le log texte

---

## Scripts de migration standalone (CLI)

Les scripts dans `audit/scripts/` permettent de migrer via CLI sans l'interface Gradio. Voir `audit/scripts/MIGRATION.md` pour le guide complet.

```bash
# Étape 0 — Créer le laboratoire
python audit/scripts/test_laboratories_creation.py

# Étape 1 — Migrer les zones (xlkey.temp_zones → /soil-sampling/units)
python audit/scripts/test_sample_units_migration.py --value 681 --dry-run
python audit/scripts/test_sample_units_migration.py --value 681

# Étape 2 — Migrer les points (xlkey.temp_points_analyse → /soil-sampling/units)
python audit/scripts/test_sample_units_points_migration.py --value 681

# Étape 3 — Migrer campagnes, échantillons, résultats
python audit/scripts/test_campaigns_samples_and_results_migration.py --dry-run
python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 9999

# Correction des sample_label post-migration
python audit/scripts/fix_sample_labels.py --dry-run
python audit/scripts/fix_sample_labels.py
```

Les fichiers de sortie sont générés dans `audit/scripts/output/` (gitignorés).

---

## Structure du projet

```
gradio_data_bridge/
├── app.py                        # Point d'entrée Gradio (5 onglets)
├── config.py                     # Configuration via .env
├── requirements.txt
├── .env.example
├── api/
│   ├── client.py                 # Client HTTP (token / compte de service / login)
│   └── writer.py                 # post_record() + registre des endpoints
├── database/
│   ├── connector.py              # Connexions psycopg2 (auto-reconnect SSL)
│   ├── schema.py                 # Découverte des métadonnées (pg_catalog)
│   ├── reader.py                 # Lecture paginée
│   └── writer.py                 # Écriture avec mapping
├── migration/
│   ├── mapper.py                 # Configuration de mapping (Pydantic)
│   ├── engine.py                 # Moteur DB + API
│   └── tracker.py                # Tables _mapping_<table>
├── audit/
│   ├── logger.py                 # Journalisation JSON
│   └── scripts/
│       ├── MIGRATION.md          # Guide de migration soil sampling
│       ├── soil_sampling_runner.py          # Runner Gradio (psycopg2)
│       ├── test_laboratories_creation.py
│       ├── test_sample_units_migration.py
│       ├── test_sample_units_points_migration.py
│       ├── test_campaigns_samples_and_results_migration.py
│       ├── fix_sample_labels.py
│       ├── data/                 # Fichiers de référence (Postman, etc.)
│       └── output/               # Fichiers générés — ignorés par git
└── ui/
    ├── components.py             # Composants Gradio réutilisables
    ├── callbacks.py              # Logique des événements (5 onglets)
    └── tabs.py                   # Layout des 5 onglets
```

## Dépendances

| Package | Usage |
|---|---|
| `gradio` | Interface web |
| `psycopg2-binary` | Driver PostgreSQL (app Gradio + runner) |
| `asyncpg` | Driver PostgreSQL async (scripts CLI standalone) |
| `pandas` | Manipulation des données |
| `requests` | Appels HTTP vers l'API xlhub |
| `python-dotenv` | Chargement de `.env` |
| `pydantic` | Validation de la configuration de mapping |
| `loguru` | Logging applicatif |
