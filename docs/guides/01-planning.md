# Guide 01: Project Planning, Infrastructure & Initialization

This guide details the foundational phase of the Geo Weather Lake project. It covers the architectural strategy, infrastructure setup with Docker, dependency management, and quality control workflows.

---

## 1. Planning & Strategy

### The Philosophy

> _"We often visualize the result and feel happy about it, but remain vague about the process."_

Planning transforms an abstract idea into an executable roadmap. For this project, I adopted a two-tier planning approach:

1.  The Blueprint: Defining scope, objectives, and high-level architecture.
2.  The Engineering Plan: Breaking down large phases into small, executable steps ("Small Wins") to maintain momentum.

### Stakeholder Analysis & Scoping

To avoid "over-engineering," I acted as the primary stakeholder to define realistic constraints, for example:

- Data Volume: Hourly weather data for ~34 locations over 1 year creates a dataset < 500MB.
- Decision: Distributed computing frameworks (e.g., Spark) are unnecessary and cost-inefficient.
- Solution: A Single-Node Architecture using Pandas (for transformation) and DuckDB (for OLAP queries) provides the best balance of performance and simplicity.

### Project Initialization

To start the project locally:

```bash
# Initialize Git repository
git init

# Add remote origin (replace with your repo URL)
git remote add origin https://github.com/tanmaivan/geo-weather-lake.git

# Create and switch to a development branch
git checkout -b dev
```

> Resource: A public template of my planning process is available here: [Project Plan Template](https://tmv-project-plan.notion.site/project-plan-template)

---

## 2. Naming Conventions

To ensure maintainability and consistency across the full stack (Python, SQL/dbt, Infrastructure), the following conventions are strictly enforced:

| Entity           | Format                                                       | Example                         |
| :--------------- | :----------------------------------------------------------- | :------------------------------ |
| MinIO Buckets    | `kebab-case`                                                 | `weather-bronze`                |
| dbt Models       | `snake_case`                                                 | `fact_hourly_weather`           |
| Python Variables | `snake_case`                                                 | `weather_df`                    |
| Python Functions | `snake_case` (Verb-Noun)                                     | `fetch_weather_data`            |
| Environment Vars | `UPPER_SNAKE_CASE`                                           | `WEATHERBIT_API_KEY`            |
| Commit Messages  | [Conventional Commits](https://www.conventionalcommits.org/) | `feat: add deduplication logic` |

---

## 3. Repository & Environment Setup

### 3.1. Prerequisites

1.  Docker Desktop: Essential for container management.
2.  Weatherbit API Key: Required for data ingestion.
3.  Conda: For local python environment isolation.

```bash
# Create and activate local environment
conda create --prefix ./venv python=3.11 -y
conda activate ./venv
```

### 3.2. Handling Offline Dependencies

To enable DuckDB to read Delta Tables from MinIO inside a container, specific extension - `delta` is required. However, containers often face connectivity issues when downloading extensions at runtime.

Solution: Pre-download extensions on the host machine and bake them into the Docker image.

**1. Create directory structure**:

```bash
mkdir -p airflow/extensions metabase
touch airflow/Dockerfile airflow/requirements.txt metabase/Dockerfile
```

**2. Download DuckDB Extensions**:
Download the `.gz` files for your DuckDB version (e.g., v1.1.x) and platform (`linux_amd64`) into `airflow/extensions/`:

```
delta.duckdb_extension.gz
```

_(Source: `http://extensions.duckdb.org/v1.4.2/linux_amd64/delta.duckdb_extension.gz`)_

### 3.3. Python Dependencies (`airflow/requirements.txt`)

We pin versions to ensure reproducibility and compatibility.

```
pandas==2.1.4       # Data manipulation
requests==2.32.5    # API calls
boto3==1.40.61      # S3/MinIO interaction
deltalake==1.2.1    # Delta Lake IO
duckdb==1.4.2       # OLAP Engine
dbt-duckdb==1.10.0  # dbt adapter
pyarrow==18.1.0     # Columnar memory format
```

---

## 4. Docker Infrastructure

We use a custom setup involving manually built images and a tailored `docker-compose.yml`.

### 4.1. Custom Docker Images

Instead of pulling generic images, we build custom ones to include necessary drivers and extensions.

- Airflow: Based on the official image but includes `dbt`, `duckdb`, and pre-loaded extensions.
- Metabase: Based on a Debian image (instead of Alpine) to support the DuckDB driver (via [MotherDuck driver](https://github.com/motherduckdb/metabase_duckdb_driver)).

Build Command:

```bash
# Build Airflow image
docker build --no-cache -t local-airflow:latest ./airflow

# Build Metabase image
docker build --no-cache -t local-metabase:latest ./metabase
```

### 4.2. Docker Compose Configuration

We start with the official Airflow docker-compose file and modify it for our Lakehouse needs.

Download Base File:

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.2/docker-compose.yaml'
```

Key Modifications:

**1. Use Custom Image**
Update `x-airflow-common` to use `local-airflow:latest`.

```
x-airflow-common: &airflow-common
  image: local-airflow:latest
  # ...
```

**2. Disable Examples**
Keep the environment clean.

```
AIRFLOW__CORE__LOAD_EXAMPLES: "false"
```

**3. MinIO Integration & Volumes**
Add environment variables for MinIO connection and mount local directories for code persistence.

```
environment:
  # ...
  AWS_ACCESS_KEY_ID: minio
  AWS_SECRET_ACCESS_KEY: minio123
  AWS_ENDPOINT_URL: "http://minio:9000"
  AWS_DEFAULT_REGION: us-east-1
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./dbt:/opt/airflow/dbt
  - ./scripts:/opt/airflow/scripts
  - ./resources:/opt/airflow/resources
  # ... other logs/config mounts
```

**4. Add MinIO Service**

```
minio:
  image: minio/minio:latest
  command: server /data --console-address ":9001"
  ports:
    - "9000:9000"
    - "9001:9001"
  environment:
    MINIO_ROOT_USER: minio
    MINIO_ROOT_PASSWORD: minio123
  volumes:
    - minio-data:/data
```

**5. Add Metabase Service**

```
metabase:
  image: local-metabase:latest
  ports:
    - "3000:3000"
  volumes:
    - ./dbt:/app/dbt # Mount dbt/duckdb file
    - metabase-data:/metabase-data # Persist Metabase config
  environment:
    - MB_DB_TYPE=h2
    - MB_DB_FILE=/metabase-data/metabase.db
```

Start the Infrastructure:

```bash
docker-compose up -d
```

---

## 5. Quality Control: Pre-commit Hooks

Pre-commit automates quality control, ensuring code consistency and security before it enters the codebase.

### Installation

```bash
pip install pre-commit
```

### Configuration (`.pre-commit-config.yaml`)

Create this file in the root directory:

```yaml
default_language_version:
  python: python3.11

repos:
  # 1. Basic File Hygiene
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v6.0.0
    hooks:
      - id: check-added-large-files
      - id: check-json
      - id: check-merge-conflict
      - id: check-yaml
      - id: end-of-file-fixer
      - id: trailing-whitespace

  # 2. Python Code Formatter (Black)
  - repo: https://github.com/psf/black
    rev: 25.11.0
    hooks:
      - id: black

  # 3. Python Linter (Ruff)
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.14.6
    hooks:
      - id: ruff
        args: [--fix]

  # 4. Secret Detection (Gitleaks)
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.29.1
    hooks:
      - id: gitleaks
```

### Activation

```bash
# Install hooks to .git/hooks
pre-commit install

# Run checks on all files manually (first run)
pre-commit run --all-files
```

---

Next Step: Building the Bronze Ingestion Pipeline

< [Back to README](../../README.md) | [Next: Ingestion >](02-ingestion.md)
