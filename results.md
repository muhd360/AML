# AML Lakehouse — Results Log

> Update this file as you go. Every number on the resume must trace back here.

## Data ingestion

| Metric | Value | Date |
|---|---|---|
| Transactions sent by producer | — | — |
| Bronze row count | — | — |
| Silver row count | — | — |
| Quarantined rows | — | — |
| Quarantine reasons | — | — |

## OpenSanctions lists

| List | Row count | Download date |
|---|---|---|
| Sanctions targets | — | — |
| PEPs | — | — |

## Screening

| Metric | Value |
|---|---|
| Candidate pairs after blocking | — |
| Chosen threshold | — |
| Precision at threshold | — |
| Recall at threshold | — |

### Recall by variant

| Variant | TP | Total | Recall |
|---|---|---|---|
| exact | — | — | — |
| typo | — | — | — |
| reorder | — | — | — |
| drop_middle | — | — | — |
| transliteration | — | — | — |

### Decoys flagged at chosen threshold
— / 300

## Pattern detection

| Metric | Value |
|---|---|
| Pattern precision | — |
| Pattern recall | — |
| Alerts per day | — |

## Performance

| Table | Query time (median) | Files read | Files pruned |
|---|---|---|---|
| txn_baseline | — | — | — |
| txn_clustered | — | — | — |

| Metric | Value |
|---|---|
| Bronze file count before OPTIMIZE | — |
| Bronze file count after OPTIMIZE | — |
| Micro-batch duration avg | — |
| Micro-batch duration peak | — |

## Cost

| Resource | Cost (USD) |
|---|---|
| Azure total | — |
| Databricks DBUs | — |

## Thresholds and why

- **FAN_IN_N**: — (99.9th percentile of distinct senders/receiving account/day)
- **FAN_OUT_N**: — (99.9th percentile of distinct receivers/sending account/day)
- **Structuring window**: 9,000–10,000, min 3 transactions/24 h
