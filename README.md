# Credit Risk Feature Store Tutorial using Feast, dbt, PostgreSQL & Redis

This project is a complete, production-style walkthrough of how to build a **real-time feature store** using [Feast](https://docs.feast.dev/), [dbt](https://docs.getdbt.com/), and the **Lending Club loan dataset**.

It demonstrates the full journey:

* Fetching raw data from Kaggle
* Cleaning and transforming data using Python + dbt
* Loading into PostgreSQL
* Materializing features with Feast into Redis
* Performing real-time lookups with just a few lines of code

---

## Dataset Used

* **Source**: [Kaggle - Lending Club](https://www.kaggle.com/datasets/wordsforthewise/lending-club)
* **Years Covered**: 2007–2018
* **Objective**: Use individual loan data to create features for credit risk models such as:

  * Loan amount
  * Interest rate
  * FICO score (bucketized)
  * State-level reject counts (geo-aggregates)

Due to the lack of `customer_id`, all features are engineered at the **loan level**, using `loan_id` as the primary entity.

---

## Project Structure

```
credit-risk-feature-store-tutorial/
├── data/                      # Raw and filtered dataset handlers
│   ├── raw/                   # Original Kaggle files
│   ├── raw_filtered/          # Cleaned and usable files
│   ├── kaggle_fetch.py        # Downloads data using Kaggle API
│   └── filter_data.py         # Preprocesses and filters necessary columns
│
├── db/
│   ├── init/                  # SQL for schema creation if needed
│   └── load_to_postgres.py    # Loads cleaned CSVs into Postgres
│
├── dbt/                       # dbt project (created via `dbt init`)
│   ├── models/
│   │   ├── staging/           # Cleaned models from raw tables
│   │   └── features/          # Final models consumed by Feast
│   ├── macros/                # Custom macros for feature logic
│   ├── seeds, snapshots, etc.
│   └── dbt_project.yml
│
├── lending_club_features/     # Feast repo
│   ├── feature_store.yaml     # Feast config (Postgres + Redis)
│   └── feature_repo/
│       ├── entities.py
│       ├── feature_views.py
│       └── feature_services.py
│
├── notebook/                  # Jupyter notebooks (if any)
├── logs/                      # Logs or experiment metadata
├── .env                       # Environment variables
├── docker-compose.yml         # Postgres + Redis services
├── requirements.txt
├── README.md
└── query_online.py            # Feature lookup script using Redis
```

---

## How Each Script Works

| Script / Component     | Purpose                                                 |
| ---------------------- | ------------------------------------------------------- |
| `kaggle_fetch.py`      | Pulls Lending Club data from Kaggle using API           |
| `filter_data.py`       | Cleans + formats data for modeling                      |
| `load_to_postgres.py`  | Loads data into PostgreSQL (offline store)              |
| `dbt/models/staging/`  | Cleans + standardizes schema                            |
| `dbt/models/features/` | Generates feature tables (used by Feast)                |
| `feature_repo/*.py`    | Defines entities, feature views, and services for Feast |
| `query_online.py`      | Runs real-time lookup from Redis using Feast            |

---

## Cloning the Repository

```bash
git clone https://github.com/venkateshtantravahi/credit-risk-feature-store-tutorial.git
cd credit-risk-feature-store-tutorial
```

---

## Setting Up the Environment

### 1. Configure `.env`

Create a `.env` file in the root directory with the following:

```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_DB=credit_db
POSTGRES_PORT=5432
REDIS_PORT=6379
KAGGLE_USERNAME=<your-kaggle-username>
KAGGLE_KEY=<your-kaggle-api-key>
```

> Make sure Kaggle API access is set up: [https://www.kaggle.com/docs/api](https://www.kaggle.com/docs/api)

### 2. Start Docker containers

```bash
docker-compose up -d
```

This runs:

* `postgres:14.9` on `${POSTGRES_PORT}`
* `redis:6.2` on `${REDIS_PORT}`

### 3. Create and activate Python environment

```bash
conda create -n feast_feature_store python=3.11
conda activate feast_feature_store

pip install -r requirements.txt
```

---

## Running the Pipeline

### 1. Load and process data

```bash
python data/kaggle_fetch.py
python data/filter_data.py
python db/load_to_postgres.py
```

### 2. Set up dbt

```bash
cd dbt
dbt init .
dbt build
```

### 3. Apply Feast definitions

```bash
cd lending_club_features
feast apply
```

### 4. Materialize features

```bash
feast materialize 2007-01-01T00:00:00 2011-12-31T23:59:59
```

### 5. Query online features

```bash
python query_online.py
```

> Ensure you're using a valid `loan_id` and `state` present in the dataset.

---

## Customization Tips

* Add new features inside `dbt/models/features/`
* Extend entities (e.g., state-level aggregations)
* Use `feast materialize-incremental` for incremental updates
* Switch online/offline stores (Redis → DynamoDB, Postgres → BigQuery)

---

## Final Thoughts

This project provides a real-world example of combining modern data engineering tools to power machine learning feature workflows.

> From raw data to real-time serving — this tutorial gives you an end-to-end pipeline, built on open-source technologies.

---

## Resources

* [Feast Docs](https://docs.feast.dev/)
* [dbt Docs](https://docs.getdbt.com/)
* [PostgreSQL](https://www.postgresql.org/)
* [Redis](https://redis.io/)
* [Kaggle Lending Club Dataset](https://www.kaggle.com/datasets/wordsforthewise/lending-club)

---

## Author

Built by [Venkatesh Tantravahi](https://github.com/venkateshtantravahi) — feel free to open an issue or PR if you'd like to collaborate or improve this repo!
