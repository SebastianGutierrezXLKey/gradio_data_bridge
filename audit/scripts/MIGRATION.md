# CrossMigrate — Guide de migration Soil Sampling

Migration des données d'échantillonnage de sol depuis la base de données legacy `xlkey` vers l'API xlhub.

---

## Vue d'ensemble

La migration s'exécute en 3 étapes dans l'ordre suivant :

```
1. Zones (unités d'échantillonnage)    →  test_sample_units_migration.py
2. Points (unités d'échantillonnage)   →  test_sample_units_points_migration.py
3. Campagnes + Échantillons + Résultats →  test_campaigns_samples_and_results_migration.py
```

L'étape 3 requiert les fichiers de mapping produits par les étapes 1 et 2.

---

## Prérequis

- Python `.venv` avec `asyncpg`, `requests`, `python-dotenv` installés
- Fichier `.env` à la racine du projet (voir `.env.example`)
- Laboratoire créé via `test_laboratories_creation.py`
- BD source accessible via les variables `SOURCE_*`
- API xlhub accessible via les variables `API_*`

---

## Étape 0 — Créer le Laboratoire

```bash
python audit/scripts/test_laboratories_creation.py
```

**Source :** variables `.env` (`LAB_NAME`, `LAB_CODE`, etc.)
**API cible :** `POST /soil-sampling/laboratories`

---

## Étape 1 — Migrer les Zones

```bash
# Simulation (aucune écriture API)
python audit/scripts/test_sample_units_migration.py --value 681 --dry-run

# Run réel (filtré par id de compte)
python audit/scripts/test_sample_units_migration.py --value 681

# Tous les comptes
python audit/scripts/test_sample_units_migration.py
```

**Table source :** `xlkey.temp_zones`
**Colonnes source :** `id, farm_id, field_id, year_key, zone_name, name, sampling_name, area, geometry`
**Jointure :** `xlkey.fields f ON f.id = tz.field_id` (pour obtenir le nom du champ)
**API cible :** `POST /soil-sampling/units` (`unit_type: zone`)

### Transformation du nom

```
zone_name_2 = tz.zone_name || '_' || f.name
# ex: zone_name="5", field.name="FR01" → "5_FR01"
```

### Traitement de la géométrie (SQL)

```sql
ST_AsGeoJSON(
  ST_Multi(
    ST_CollectionExtract(
      ST_MakeValid(ST_CurveToLine(tz.geometry)),
      3  -- extraire uniquement les polygones
    )
  )
)
```

Gère les géométries courbes `MultiSurface` et les géométries invalides de la source.

### Fichier de mapping produit

`audit/scripts/output/sample_units_mapping_YYYYMMDD_HHMMSS.json`

```json
{
  "source_id": "50",
  "target_api_id": "12",
  "field_id": "3267",
  "zone_name_2": "2_FR01",
  "unit_type": "zone",
  "source_table": "xlkey.temp_zones"
}
```

### Annulation (downgrade)

```bash
python audit/scripts/test_sample_units_migration.py --downgrade
```

---

## Étape 2 — Migrer les Points

```bash
# Filtré par id de compte
python audit/scripts/test_sample_units_points_migration.py --value 681

# Tous les points
python audit/scripts/test_sample_units_points_migration.py
```

**Table source :** `xlkey.temp_points_analyse`
**Chemin de jointure :** `xlkey.accounts → xlkey.fields → xlkey.temp_points_analyse`
**API cible :** `POST /soil-sampling/units` (`unit_type: point`)

### Transformation du nom

```
1. Retirer le préfixe (défaut "ROY") du samp_name
2. Inverser [nom]_[id] → [id]_[nom]

Exemples (préfixe="ROY") :
  "ROYGUAY58_1" → retrait → "GUAY58_1" → inversion → "1_GUAY58"
  "02_7"        → aucun retrait         → inversion → "7_02"
```

### Fichier de mapping produit

`audit/scripts/output/sample_units_points_mapping_YYYYMMDD_HHMMSS.json`

```json
{
  "source_id": "15857",
  "target_api_id": "24",
  "samp_name_raw": "ROYGUAY58_1",
  "name": "1_GUAY58",
  "unit_type": "point",
  "source_table": "xlkey.temp_points_analyse"
}
```

### Arguments

| Argument | Défaut | Description |
|---|---|---|
| `--col-name` | `id` | Colonne de `xlkey.accounts` pour filtrer |
| `--value` | (tous) | Valeur du filtre |
| `--prefix` | `ROY` | Préfixe à retirer du samp_name |

### Annulation (downgrade)

```bash
python audit/scripts/test_sample_units_points_migration.py --downgrade
```

---

## Étape 3 — Migrer Campagnes, Échantillons & Résultats de Lab

```bash
# Simulation (5 lignes)
python audit/scripts/test_campaigns_samples_and_results_migration.py --dry-run

# Run réel
python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 9999

# Filtré par nom de fichier
python audit/scripts/test_campaigns_samples_and_results_migration.py --limit 100 --filename-filter "681"
```

**Table source :** `xlkey.temp_analyses`
**API cible :** 4 POSTs séquentiels par ligne

### Pipeline par ligne

```
1. Campagne    POST /soil-sampling/campaigns
2. Import      POST /soil-sampling/imports
3. Échantillon POST /soil-sampling/samples
4. Résultat    POST /soil-sampling/results
```

### Résolution des unités (mapping)

Le script fusionne les deux fichiers de mapping pour résoudre `FIELD` → `sampling_unit_id` :

```
Colonne FIELD (ex: "FR01_5")
→ field_to_zone_name_2("FR01_5") = "5_FR01"
→ lookup dans le mapping fusionné (zones + points)
→ target_api_id
```

La clé de lookup est `zone_name_2` (zones) ou `name` (points).

### Déduplication

| Entité | Clé de dédup | Stratégie |
|---|---|---|
| Campagne | `name` (= `"Campaign YYYY-MM-DD"`) | Pré-fetchée depuis l'API avant la boucle |
| Import | `filename` | Pré-fetché depuis l'API avant la boucle |
| Échantillon | aucune | Toujours créé |
| Résultat de lab | aucune | Toujours créé |

### Mapping des champs de résultats de lab

| Colonne source | Champ API |
|---|---|
| PH | ph_water |
| PH_T | ph_buffer |
| MO | organic_matter_percent |
| P | phosphorus_kg_ha |
| K | potassium_kg_ha |
| CA | calcium_kg_ha |
| MG | magnesium_kg_ha |
| AL | aluminum_ppm |
| SATURATION_P | phosphorus_saturation_index |
| CEC_MEQ | cec_meq_100g |
| BORE | boron_ppm |
| MN | manganese_ppm |
| CU | copper_ppm |
| ZN | zinc_ppm |
| FE | iron_ppm |
| S | sulfur_ppm |

### Fichier de sortie

`audit/scripts/output/campaigns_migration_YYYYMMDD_HHMMSS.json`

```json
{
  "source_id": "2298",
  "zone_name_2": "1_FR01",
  "unit_type": "zone",
  "FIELD_raw": "FR01_1",
  "campaign_id": "4",
  "import_id": "2",
  "sample_id": "1",
  "lab_result_id": "1"
}
```

### Annulation (downgrade)

```bash
python audit/scripts/test_campaigns_samples_and_results_migration.py --downgrade
```

Suppression dans l'ordre inverse : `résultats → échantillons → imports → campagnes`

---

## Correction des étiquettes d'échantillons (patch post-migration)

Si des échantillons ont été créés avec un `sample_label` incorrect :

```bash
python audit/scripts/fix_sample_labels.py --dry-run
python audit/scripts/fix_sample_labels.py
```

Applique un PATCH sur chaque `sample_label` au format `FIELD_raw` (`[champ]_[no_echantillon]`, ex: `FR01_5`).

---

## Authentification

Trois niveaux de priorité (le premier disponible est utilisé) :

1. `API_TOKEN` — Bearer token fixe dans `.env`
2. `API_CLIENT_ID` + `API_CLIENT_SECRET` — compte de service via `POST /api/v1/service-accounts/token`
3. `API_LOGIN_EMAIL` + `API_LOGIN_PASSWORD` — connexion par courriel/mot de passe

---

## Contraintes connues

- `zone_name_2` n'est **pas unique** dans la source — le même numéro de zone peut apparaître pour plusieurs `year_key` (ex: plusieurs enregistrements `5_FR01`). Seul le premier par nom est migré ; les suivants sont ignorés (SKIP) par le contrôle de déduplication.
- Les géométries source de type `MultiSurface` ou `GeometryCollection` sont normalisées en `MultiPolygon` via `ST_Multi(ST_CollectionExtract(..., 3))`.
- L'API xlhub auto-découvre les liens `field_id` par intersection spatiale lors de la création d'un `SamplingUnit` — pas besoin de passer les field_ids explicitement.
- Les fichiers de sortie sont gitignorés (`audit/scripts/output/`). Conservez-les localement entre les étapes 1 et 3.
