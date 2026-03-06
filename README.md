# CrossMigrate

Application Gradio pour migrer des données entre deux bases PostgreSQL avec des schémas différents, avec support d'un mode d'écriture direct en base ou via une API REST (JWT Bearer).

## Fonctionnalités

- **Connexion sécurisée** aux bases source et cible (via variables d'environnement)
- **Découverte automatique** des tables, colonnes, types et clés étrangères (`pg_catalog`)
- **Visualisation côte à côte** des données source et cible
- **Mapping manuel** des colonnes (source → cible) et des valeurs (ex : IDs de FK)
- **Mode Dry Run** : simulation complète sans écriture
- **Migration par lots** avec suivi de progression en temps réel
- **Mode d'écriture DB** : INSERT direct avec tables de mapping `_mapping_<table>`
- **Mode d'écriture API** : POST via HTTP avec authentification JWT Bearer
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
# Base source
SOURCE_HOST=localhost
SOURCE_PORT=5432
SOURCE_DB=ma_base_source
SOURCE_USER=postgres
SOURCE_PASSWORD=mon_mot_de_passe
SOURCE_SCHEMA=public

# Base cible (mode DB direct)
TARGET_HOST=prod-server.example.com
TARGET_PORT=5432
TARGET_DB=ma_base_cible
TARGET_USER=postgres
TARGET_PASSWORD=autre_mot_de_passe
TARGET_SCHEMA=public

# API cible (mode API)
API_BASE_URL=http://localhost:8000
API_VERSION=/api/v1
API_LOGIN_ENDPOINT=/auth/login
API_LOGIN_EMAIL=user@example.com
API_LOGIN_PASSWORD=mon_mot_de_passe
# Ou token fixe (optionnel, prioritaire sur login)
API_TOKEN=

# Laboratoire (script test_laboratories_creation.py)
LAB_NAME=MonLaboratoire
LAB_CODE=LAB-001
LAB_ADDRESS=123 rue des Sciences, Québec, QC
LAB_CONTACT_EMAIL=info@monlab.com
LAB_CONTACT_PHONE=+1-418-555-0100
LAB_COUNTRY=Canada
```

## Lancement de l'application

```bash
python app.py
# → Ouvre http://localhost:7860
```

## Workflow d'utilisation (Application Gradio)

### Onglet 1 — Connexion
1. Remplissez les paramètres de connexion **source** (PostgreSQL)
2. Choisissez le mode d'écriture : **DB directe** ou **API**
   - Mode DB : remplissez les paramètres cible
   - Mode API : remplissez l'URL de base, version et identifiants
3. Testez la connexion avec le bouton dédié

### Onglet 2 — Sélection & Visualisation
1. Cliquez **"Rafraîchir les tables"** pour lister les tables disponibles
2. Sélectionnez une table source et une table cible
3. Visualisez les données côte à côte
4. Consultez le nombre de lignes, colonnes et clés primaires

### Onglet 3 — Mapping
1. Cliquez **"Charger le mapping"** après avoir sélectionné les tables
2. **Mapping des colonnes** : associez chaque colonne source à une colonne cible (ou "— Ne pas migrer —")
3. **Mapping des valeurs FK** : pour les clés étrangères, chargez les valeurs distinctes et remplissez les IDs cibles
4. Cliquez **"Valider la configuration"** pour vérifier l'absence d'erreurs

### Onglet 4 — Migration
1. Choisissez le mode : **"Dry Run"** (recommandé en premier) ou **"Réel"**
2. Choisissez le mode d'écriture : **DB** ou **API** (+ endpoint si API)
3. Ajustez la taille de lot si nécessaire
4. Cliquez **"Lancer la migration"**
5. Suivez les logs en temps réel
6. Téléchargez le fichier **audit JSON** à la fin

---

## Scripts de migration standalone

Les scripts dans `audit/scripts/` permettent de migrer des entités spécifiques directement via l'API xlhub, sans passer par l'interface Gradio.

Les fichiers de sortie (JSON) sont générés dans `audit/scripts/output/` avec un horodatage dans le nom, et sont ignorés par git.

### `test_laboratories_creation.py` — Création d'un laboratoire

Crée un laboratoire dans l'API xlhub (`POST /soil-sampling/laboratories`). Vérifie qu'il n'existe pas déjà avant de le créer.

```bash
# Créer le laboratoire (paramètres dans .env)
python audit/scripts/test_laboratories_creation.py

# Supprimer le laboratoire créé (utilise le dernier fichier dans output/)
python audit/scripts/test_laboratories_creation.py --downgrade

# Spécifier un fichier de record particulier
python audit/scripts/test_laboratories_creation.py --downgrade --record-file audit/scripts/output/laboratory_record_20260306_143022.json
```

Fichier de sortie : `audit/scripts/output/laboratory_record_YYYYMMDD_HHMMSS.json`
```json
{ "lab_id": "42", "name": "MonLaboratoire", "code": "LAB-001" }
```

---

### `test_sample_units_migration.py` — Migration des zones d'échantillonnage

Migre les zones de `xlkey.sampling_zone_2` vers l'API xlhub (`POST /soil-sampling/units`).

- Filtre optionnel par nom de compte (`--value`)
- Vérifie les doublons avant envoi (par `zone_name_2` + `FIELD_NAME`)
- Sauvegarde un fichier de mapping `source_id → target_api_id`

```bash
# Dry run — filtrer par nom de compte contenant "9206"
python audit/scripts/test_sample_units_migration.py --value "9206" --dry-run

# Migration réelle
python audit/scripts/test_sample_units_migration.py --value "9206"

# Migrer toutes les zones (sans filtre)
python audit/scripts/test_sample_units_migration.py

# Supprimer les unités créées (utilise le dernier fichier dans output/)
python audit/scripts/test_sample_units_migration.py --downgrade

# Arguments disponibles
#   --col-name     Colonne de xlkey.accounts à filtrer (défaut : name_en)
#   --value        Valeur recherchée avec ILIKE (optionnel)
#   --email        Email API (défaut : API_LOGIN_EMAIL dans .env)
#   --password     Mot de passe API (défaut : API_LOGIN_PASSWORD dans .env)
#   --dry-run      Simulation sans appel API
#   --downgrade    Supprime les unités créées via le fichier de mapping
#   --mapping-file Chemin explicite vers le fichier de mapping
```

Fichier de sortie : `audit/scripts/output/sample_units_mapping_YYYYMMDD_HHMMSS.json`
```json
[
  {
    "source_id": "1234",
    "target_api_id": "abc-uuid",
    "FIELD_NAME": "Champ Nord",
    "zone_name_2": "Zone A"
  }
]
```

---

## Structure du projet

```
gradio_data_bridge/
├── app.py                        # Point d'entrée Gradio
├── config.py                     # Configuration via .env
├── requirements.txt
├── .env.example
├── api/
│   ├── client.py                 # Client HTTP JWT Bearer
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
│       ├── test_laboratories_creation.py
│       ├── test_sample_units_migration.py
│       ├── data/                 # Fichiers de référence (Postman, etc.)
│       └── output/               # Fichiers générés — ignorés par git
└── ui/
    ├── components.py             # Composants Gradio réutilisables
    ├── callbacks.py              # Logique des événements
    └── tabs.py                   # Layout des 4 onglets
```

## Dépendances

| Package | Usage |
|---|---|
| `gradio` | Interface web |
| `psycopg2-binary` | Driver PostgreSQL (app Gradio) |
| `asyncpg` | Driver PostgreSQL async (scripts standalone) |
| `pandas` | Manipulation des données |
| `requests` | Appels HTTP vers l'API |
| `python-dotenv` | Chargement de `.env` |
| `pydantic` | Validation de la configuration de mapping |
| `loguru` | Logging applicatif |
