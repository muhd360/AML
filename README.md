# Real-Time AML Monitoring Lakehouse

**Azure Databricks · PySpark Structured Streaming · Event Hubs · Delta Lake · Unity Catalog · GitHub Actions**

A portfolio project that streams the IBM AML transactions dataset through a Bronze/Silver/Gold Delta lakehouse, screens party names against OpenSanctions, and detects laundering patterns in real time.

> **Data notice:** IBM transactions dataset and OpenSanctions bulk data are used for personal, non-commercial learning purposes only.

---

## Architecture

*(Add draw.io / Excalidraw PNG here)*

---

## Results

*(Fill from results.md once runs are complete)*

---

## How to run

### Prerequisites

| Tool | Version |
|---|---|
| Python | 3.11 |
| Java | 17 |
| Azure subscription | — |
| Databricks workspace (Premium) | — |

### 1. Clone and install

```bash
git clone <repo>
cd aml-lakehouse
python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Download the data

```bash
kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -f HI-Small_Trans.csv
kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml -f HI-Small_Patterns.txt
```

Download OpenSanctions sanctions and PEPs CSVs from opensanctions.org/datasets.

### 3. Set up Azure resources

Follow the setup order in the build guide: resource group → ADLS Gen2 → Databricks Premium → Event Hubs Standard → Key Vault → secret scope.

### 4. Set environment variables

```bash
export EH_SEND_CONN="<Event Hubs Send connection string>"
```

### 5. Run the pipeline

1. Upload patterns file and OpenSanctions CSVs to `/Volumes/aml/ref/raw/`
2. Start the Bronze stream: `notebooks/01_bronze_stream.py`
3. Start the Silver stream: `notebooks/02_silver_stream.py`
4. Run the producer: `python producer/send_transactions.py`
5. Run synthetic names (once): `notebooks/03a_synthetic_names.py`
6. Run screening batch: `notebooks/03_screening_batch.py`
7. Start rule detection stream: `notebooks/04_rules_stream.py`
8. Start alerts stream: `notebooks/05_gold_alerts.py`
9. Run evaluation: `notebooks/06_evaluation.py`

### 6. Run tests locally

```bash
PYTHONPATH=src pytest tests/
```

---

## Screenshots

*(Add: masked vs unmasked query, lineage graph, green CI run)*

---

## Limitations

- Synthetic party names; screening test is self-designed against planted names
- IBM dataset is synthetic; pattern thresholds are dataset-specific
- Structuring rule has no IBM ground-truth label (scored overall only)

## What I'd do next

- ML model to rank alerts by risk score
- CDC (Debezium) instead of watermark-based incremental loads
- Case-management system feed for analyst workflow
