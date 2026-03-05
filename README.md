# CrossMigrate

Application Gradio pour migrer des données entre deux bases PostgreSQL avec des schémas différents.

## Fonctionnalités

- **Connexion sécurisée** aux bases source et cible (via variables d'environnement)
- **Découverte automatique** des tables, colonnes, types et clés étrangères
- **Visualisation côte à côte** des données source et cible
- **Mapping manuel** des colonnes (source → cible) et des valeurs (ex : IDs de FK)
- **Mode Dry Run** : simulation complète sans écriture en base
- **Migration par lots** avec suivi de progression en temps réel
- **Tables de mapping temporaires** (`_mapping_<table>`) pour tracer source_id → target_id
- **Audit JSON** téléchargeable après chaque migration

## Installation

```bash
# 1. Cloner le dépôt
git clone <repo_url>
cd crossmigrate

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

# Base cible
TARGET_HOST=prod-server.example.com
TARGET_PORT=5432
TARGET_DB=ma_base_cible
TARGET_USER=postgres
TARGET_PASSWORD=autre_mot_de_passe
TARGET_SCHEMA=public

# Répertoire de sortie des audits (optionnel)
AUDIT_OUTPUT_DIR=/tmp/crossmigrate_audits
```

## Lancement

```bash
python app.py
# → Ouvre http://localhost:7860
```

## Workflow d'utilisation

### Onglet 1 — Connexion
1. Remplissez les paramètres de connexion source et cible
2. Cliquez **"Tester la connexion"** pour chaque base
3. Un badge vert confirme la connexion

### Onglet 2 — Sélection & Visualisation
1. Cliquez **"Rafraîchir les tables"** pour lister les tables disponibles
2. Sélectionnez une table source et une table cible
3. Visualisez les 20 premières lignes côte à côte
4. Utilisez **"Charger 50 lignes supplémentaires"** pour paginer

### Onglet 3 — Mapping
1. Cliquez **"Charger le mapping"** après avoir sélectionné les tables
2. **Mapping des colonnes** : éditez le tableau pour associer chaque colonne source à une colonne cible (ou "— Ne pas migrer —")
3. Cliquez **"Sauvegarder le mapping des colonnes"**
4. **Mapping des valeurs FK** : pour les clés étrangères, sélectionnez la colonne, chargez les valeurs distinctes et remplissez les IDs cibles
5. Cliquez **"Valider la configuration"** pour vérifier l'absence d'erreurs

### Onglet 4 — Migration
1. Choisissez le mode : **"Dry Run"** (recommandé en premier) ou **"Réel"**
2. Ajustez la taille de lot si nécessaire
3. Cliquez **"Lancer la migration"**
4. Suivez les logs en temps réel
5. Téléchargez le fichier **audit JSON** à la fin

## Structure du projet

```
crossmigrate/
├── app.py                    # Point d'entrée Gradio
├── config.py                 # Configuration via .env
├── requirements.txt
├── .env.example
├── database/
│   ├── connector.py          # Connexions psycopg2
│   ├── schema.py             # Découverte des métadonnées
│   ├── reader.py             # Lecture paginée
│   └── writer.py             # Écriture avec mapping
├── migration/
│   ├── mapper.py             # Configuration de mapping
│   ├── engine.py             # Moteur de migration
│   └── tracker.py            # Tables _mapping_<table>
├── audit/
│   └── logger.py             # Journalisation JSON
└── ui/
    ├── components.py         # Composants Gradio réutilisables
    ├── callbacks.py          # Logique des événements
    └── tabs.py               # Layout des 4 onglets
```

## Format du fichier d'audit JSON

```json
{
  "migration_id": "20250304_143022",
  "timestamp_start": "2025-03-04T14:30:22",
  "timestamp_end": "2025-03-04T15:45:10",
  "mode": "real",
  "source_db": "source_db_name",
  "target_db": "target_db_name",
  "tables_migrated": [
    {
      "source_table": "clients",
      "target_table": "customers",
      "records_attempted": 150,
      "records_succeeded": 148,
      "records_failed": 2
    }
  ],
  "errors": [...],
  "mappings_used": {
    "columns": { "nom": "last_name", "email": "email_address" },
    "values": { "statut": { "actif": "active", "inactif": "inactive" } }
  }
}
```

## Tables de mapping temporaires

Après une migration réelle, des tables `_mapping_<source_table>` sont créées dans la base cible :

```sql
SELECT * FROM _mapping_clients;
-- source_id | target_id | migrated_at
-- 123       | 456       | 2025-03-04 14:35:22
```

Ces tables permettent de résoudre les clés étrangères lors de migrations multi-tables.

## Dépendances

| Package | Usage |
|---|---|
| `gradio` | Interface web |
| `psycopg2-binary` | Driver PostgreSQL |
| `pandas` | Manipulation des données |
| `python-dotenv` | Chargement de `.env` |
| `pydantic` | Validation de la configuration de mapping |
| `loguru` | Logging applicatif |
