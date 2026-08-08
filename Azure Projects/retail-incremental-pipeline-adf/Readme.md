# Retail Orders — Azure Incremental Loading Pipeline

An end-to-end, production-style **incremental data loading pipeline** built entirely on Azure, using Azure Data Factory (ADF) as the orchestration engine, Azure SQL Database as the destination, and Blob Storage as the landing zone for incoming files. The pipeline is triggered two different ways (event-driven and scheduled), secured with Azure Key Vault, and deployed through a Git-based CI/CD workflow into a separate Test environment.

This is **Project 2** in a two-project PySpark/Azure learning path (Project 1 was a local PySpark batch ETL pipeline). Where Project 1 focused on *transformation logic*, this project focuses on *orchestration, infrastructure, incremental loading patterns, and DevOps practices* — the parts of data engineering that sit around the actual data processing.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Resources & Tools Used](#resources--tools-used)
3. [The Dataset](#the-dataset)
4. [Database Design](#database-design)
5. [The Incremental Loading Pattern (Watermarking)](#the-incremental-loading-pattern-watermarking)
6. [ADF Pipeline: Activity by Activity](#adf-pipeline-activity-by-activity)
7. [Trigger 1: Storage Event Trigger](#trigger-1-storage-event-trigger)
8. [Trigger 2: Tumbling Window Trigger](#trigger-2-tumbling-window-trigger)
9. [Security: Azure Key Vault Integration](#security-azure-key-vault-integration)
10. [Git Integration & Collaboration Workflow](#git-integration--collaboration-workflow)
11. [Second Environment: Test](#second-environment-test)
12. [CI/CD with GitHub Actions](#cicd-with-github-actions)
13. [Known Issues](#known-issues)
14. [Key Lessons Learned](#key-lessons-learned)
15. [Cost Management](#cost-management)

---

## Architecture Overview

```
[CSV file uploaded to Blob Storage container "raw-data"]
        │
        ├──────────────────────────────┐
        ▼                              ▼
[Storage Event Trigger]      [Tumbling Window Trigger]
   (fires per file)              (fires hourly)
        │                              │
        └──────────────┬───────────────┘
                        ▼
              [ADF Pipeline]
   1. Get Metadata   → confirm the file exists
   2. Lookup          → read current watermark from SQL
   3. Copy Data        → land file rows into staging table
   4. Stored Procedure  → MERGE staging → final table,
                          filtered by watermark, then
                          advance the watermark
                        │
                        ▼
        [Azure SQL Database: orders table]
```

Two trigger strategies feed the **same** underlying pipeline logic — this was a deliberate design choice to learn both event-driven and schedule-driven orchestration patterns using one consistent core.

---

## Resources & Tools Used

| Resource / Tool | What it is | Why we used it |
|---|---|---|
| **Azure Resource Group** | A logical container that groups related Azure resources together | Lets us manage, monitor, and delete every resource for this project as one unit, rather than tracking scattered individual resources |
| **Azure Blob Storage** | Object storage for unstructured/semi-structured files (like CSVs) | This is the "landing zone" — where incoming data files arrive before being processed. Cloud pipelines almost always have a raw storage layer like this rather than reading files directly off a server |
| **Azure SQL Database** | A managed relational database (PaaS — no server management required) | Our destination system of record. We used **Serverless/Basic tier** specifically because it auto-pauses or is cheap when idle, keeping learning-project costs low |
| **Azure Data Factory (ADF)** | A cloud orchestration/ETL service — visually build pipelines of "activities" that move and transform data | The core engine of this project. ADF doesn't process data itself in most activities (like Copy Data) — it orchestrates *other* services (Blob Storage, SQL) to do the work, and handles scheduling, retries, and monitoring |
| **Azure Event Grid** | A service that detects and routes events (like "a blob was created") | Powers the Storage Event Trigger under the hood — ADF doesn't poll Blob Storage itself; Event Grid notices the change and pushes a notification to ADF |
| **Azure Key Vault** | A managed secrets store for passwords, keys, and certificates | Removes the need to store database passwords in plaintext inside ADF's configuration — a real security best practice, not just a nice-to-have |
| **GitHub** | Version control hosting | Gives ADF's pipeline definitions proper history, code review (via Pull Requests), and a foundation for automated deployment |
| **GitHub Actions** | GitHub's built-in CI/CD automation | Automates deploying ADF's exported ARM templates into a separate Test environment, instead of manually clicking through the Azure Portal every time |
| **ARM Templates (Azure Resource Manager)** | JSON files describing Azure infrastructure/resources declaratively | This is *how* ADF's pipelines get deployed to a different environment — export from Dev, deploy the same template (with different parameters) to Test |

---

## The Dataset

We reused the same **Superstore Sales** retail dataset style from Project 1 (~8,400 orders, 21 columns: order dates, sales, profit, customer segment, product category, region, etc.), but split it into **separate files by year** (`orders_2009.csv`, `orders_2010.csv`, `orders_2011.csv`, ...) using a small local Python script.

**Why split by year?** The whole point of this project is *incremental* loading — proving that a pipeline can correctly detect and load only *new* data on each run. A single static file can't demonstrate that. Splitting by year lets us simulate "new data arriving over time" by uploading one file at a time.

---

## Database Design

Three tables, each with a distinct role:

| Table | Purpose | Key design choice |
|---|---|---|
| `orders` | The permanent, ever-growing destination table | `row_id` is the `PRIMARY KEY`, preventing duplicate inserts even if a file gets reprocessed |
| `staging_orders` | A temporary scratch table — holds only the current run's file data | **Truncated at the start of every Copy Data run** (`TRUNCATE TABLE staging_orders`), so it never accumulates history — its only job is to hold "what we're about to merge right now" |
| `watermark_control` | Tracks incremental load progress | Stores a single row: `table_name = 'orders'`, `last_loaded_date`. This is the pipeline's "memory" of where it left off |

**Why two different timestamp-like columns exist (`order_date` vs `load_timestamp`)?**
- `order_date` (in `orders`) is a fact about the **business event** — when the sale happened. It's a plain `DATE`.
- `load_timestamp` (also in `orders`) is metadata about **our pipeline's own operation** — exactly when *we* inserted this row. It's a `DATETIME DEFAULT GETDATE()`, auto-populated, useful for auditing pipeline runs.
- `watermark_control.last_loaded_date` tracks progress against `order_date` specifically (business time), not `load_timestamp` (operational time) — because "what's new" is defined by the data's own timeline, not by when we happened to run the pipeline.

---

## The Incremental Loading Pattern (Watermarking)

This is the conceptual core of the whole project. Instead of reloading all data every run (wasteful, and would create duplicates), the pipeline:

1. **Reads the current watermark** — "we've already loaded everything up to `order_date = X`"
2. **Copies the raw file(s) into staging** — no filtering yet at this stage
3. **Runs a `MERGE` statement** that only considers staging rows where `order_date > watermark` — this is where the actual filtering happens
4. **Advances the watermark** to the new maximum `order_date` just loaded, but only if it's genuinely later than before (guards against ever moving the watermark backward)

**Why filter during the SQL merge step, rather than during Copy Data itself?**
Early in the project, we discovered ADF's Copy Data activity can't cleanly apply row-level filters (like `WHERE order_date > @watermark`) when the *source* is a flat file (CSV) — that kind of filtering works well when the source is a queryable database, but not a file. So the design shifted: copy the whole file into staging, and let SQL's `MERGE ... WHERE` handle the actual incremental logic. This turned out to be a genuinely common real-world pattern too, not just a workaround.

**Why `MERGE` instead of plain `INSERT`?**
`MERGE` lets us handle both cases in one statement: `WHEN MATCHED` (row already exists — update it) and `WHEN NOT MATCHED` (genuinely new row — insert it). This makes the pipeline safe to accidentally re-run against the same file without creating duplicates.

---

## ADF Pipeline: Activity by Activity

| # | Activity | Type | What it does | Why it's needed |
|---|---|---|---|---|
| 1 | `Get_Metadata_CheckFile` | Get Metadata | Confirms the triggering file actually exists in Blob Storage | A sanity check before attempting to process a file that might not be there |
| 2 | `Lookup_GetWatermark` | Lookup | Runs `SELECT last_loaded_date FROM watermark_control WHERE table_name = 'orders'` | This is how the pipeline "remembers" where it left off between runs |
| 3 | `Copy_IncrementalRows` | Copy Data | Reads the CSV(s) from Blob Storage, writes all rows into `staging_orders` (after truncating it) | Lands the raw data where SQL can process it; incremental filtering happens *after* this step, not during |
| 4 | `SP_MergeIncremental` | Stored Procedure | Executes `usp_merge_incremental_orders` — the `MERGE` + watermark-advance logic | The actual "finalize the load" step — this is where incremental correctness is enforced |

**Datasets used:**
- `ds_blob_csv` — points to a single, parameterized CSV file (`fileName` parameter), used by the Storage Event–triggered pipeline
- `ds_blob_csv_allfiles` — points to the whole `raw-data` container using a wildcard (`*.csv`), used by the Tumbling Window–triggered pipeline
- `ds_sql_staging` — points to `staging_orders`
- `ds_sql_watermark` — points to `watermark_control`

**Linked Services used:**
- `ls_blob_storage` — connection to the Storage Account
- `AzureSqlDatabase1` — connection to Azure SQL Database (password sourced from Key Vault — see [Security](#security-azure-key-vault-integration))
- `ls_key_vault` — connection to Azure Key Vault itself

---

## Trigger 1: Storage Event Trigger

**What it is:** Fires automatically the moment a new blob is created in the `raw-data` container. Built on top of **Azure Event Grid**, which detects the storage change and notifies ADF.

**Configuration:**
- Container: `raw-data`
- Blob path ends with: `.csv`
- Event: `Blob created` only
- Pipeline parameter mapping: `triggerFileName` ← `@trigger().outputs.body.fileName` (dynamically pulls the exact filename of whatever blob just landed)

**Why this pattern:** Ideal for "process data as soon as it arrives" scenarios — low latency, no wasted runs when nothing has changed.

**Real issue hit & fixed:** Required registering the `Microsoft.EventGrid` resource provider on the subscription before Storage Event Triggers would function correctly — an easy-to-miss one-time subscription-level setup step.

---

## Trigger 2: Tumbling Window Trigger

**What it is:** Fires on a fixed recurring schedule (we used hourly, for observability during learning — production might use daily). Each run represents a specific time window, and supports backfill/replay of missed windows.

**Why it needed a pipeline variant:** Tumbling Window Triggers have no concept of "which file arrived" — they only know "what time window is this." So a separate pipeline (`pl_tumbling_window_load`) was created, using `ds_blob_csv_allfiles` (wildcard `*.csv`) instead of a single parameterized filename — it re-scans the *entire* container every run.

**Why this is still safe (not wasteful/duplicative):** Because the watermark-based `MERGE` step protects against reprocessing — even though this pipeline reads every file in the container every run, only genuinely new rows (based on `order_date > watermark`) ever get merged into `orders`. This was explicitly tested: running this pipeline with no new files uploaded resulted in zero row count change, proving the protection works.

**Key settings:**
- Recurrence: every 1 hour
- Max concurrency: 1 (prevents overlapping runs from racing against the shared watermark)

---

## Security: Azure Key Vault Integration

**The problem:** Initially, the SQL admin password was stored directly (in plaintext) inside the `AzureSqlDatabase1` Linked Service configuration — readable by anyone with access to the Data Factory resource, and a risk if the ARM template was ever exported/shared.

**The fix:**
1. Created an Azure Key Vault, stored the SQL password there as a secret (`sql-admin-password`)
2. Created a Linked Service (`ls_key_vault`) so ADF can talk to Key Vault
3. Granted ADF's **system-assigned managed identity** (essentially, ADF's own service account in Azure AD) the **Key Vault Secrets User** role — a separate grant from your own personal access
4. Reconfigured `AzureSqlDatabase1`'s password field to reference the Key Vault secret (`AzureKeyVaultSecret` type) instead of a plaintext value

**Why a managed identity, specifically:** This means ADF can retrieve the secret it needs at runtime without any human-managed credential being embedded anywhere — the "credential" is really just "ADF's own Azure identity has permission," which Azure manages automatically.

---

## Git Integration & Collaboration Workflow

ADF Studio was connected to a GitHub repository, changing how pipeline changes get made:

```
Feature branch (e.g. add-pipeline-descriptions)
        │  (edit + Save commits here)
        ▼
Pull Request → review → merge
        ▼
main branch (source of truth — human-readable ADF JSON)
        │  (click "Publish" in ADF Studio)
        ▼
adf_publish branch (auto-generated deployable ARM templates)
```

**Why three branches exist, not just one:**
- `main` — the authoring format: individual, human-reviewable JSON files per pipeline/dataset/trigger
- feature branches — isolated workspace for one change at a time
- `adf_publish` — a **generated build artifact**, not hand-edited; conceptually similar to a compiled output folder in software projects. Clicking "Publish" in ADF Studio is what triggers ADF to regenerate this branch's contents from whatever's currently in `main`

---

## Second Environment: Test

To avoid ever testing changes directly against a single set of "production-like" resources, a **second, fully isolated environment** was built:
- Separate Resource Group: `rg-retail-incremental-pipeline-test`
- Separate Storage Account, SQL Database (same table/procedure definitions), and Data Factory instance

**Deployment mechanism:** ADF's **Export ARM Template** feature produces a deployable template (plus a linked-templates structure, since the factory had grown to include multiple pipelines/datasets). This template was deployed into the Test environment's resources, with environment-specific values (connection strings, storage keys) supplied as deployment parameters — while the pipeline *logic* itself stayed identical to Dev.

---

## CI/CD with GitHub Actions

A GitHub Actions workflow (`.github/workflows/deploy-to-test.yml`) was built to automate the manual ARM template deployment described above:

```yaml
on:
  push:
    branches:
      - adf_publish
```

**What it does when triggered:**
1. Checks out the repo (at the `adf_publish` branch's state)
2. Authenticates to Azure using a dedicated **Service Principal** (a non-human "robot" identity created specifically for this automation, granted `Contributor` access scoped only to the Test resource group)
3. Deploys the exported ARM template to the Test Data Factory, injecting Test-specific parameters (like the storage account key) from **GitHub Secrets** — never hardcoded in the workflow file itself
4. Logs out

**Why trigger on `adf_publish` specifically, not `main`:** We only want deployments to happen when there's a genuinely ADF-validated, deployable artifact ready — which is exactly what a fresh commit to `adf_publish` represents (it only updates when you click "Publish" in ADF Studio, meaning ADF itself has already validated the pipeline).

**Why a Service Principal instead of personal credentials:** Automation should never run using a human's personal login — a Service Principal is a scoped, revocable, auditable identity meant specifically for automated processes like this.

---

## Known Issues

- **GitHub Actions trigger not yet firing reliably:** Despite the workflow file being correctly located at `.github/workflows/deploy-to-test.yml` on the `main` branch, syntactically valid, and `adf_publish` genuinely receiving new pushes, the workflow was not appearing under the repo's Actions tab at the time this project was paused. Suspected causes not yet fully ruled out: repository-level Actions permissions (Settings → Actions → General) potentially restricting workflow execution, or a subtler YAML/branch-matching issue. **Next debugging steps:** verify Actions permissions aren't set to "Disable Actions," and check `https://github.com/<user>/<repo>/actions/workflows/deploy-to-test.yml` directly to see if GitHub registers the workflow at all.

---

## Key Lessons Learned

Beyond the intended architecture, this project surfaced several genuinely realistic data engineering debugging scenarios:

1. **CSV escape-character quoting** — a product name containing an embedded inch-mark (`11"`) broke ADF's Copy Data parsing because the dataset's escape character was set to backslash (`\`) instead of matching the file's actual doubled-quote (`""`) escaping convention. Lesson: default CSV settings aren't always correct; verify against the actual source file's quoting style.

2. **Linked Service reference mismatches** — a dataset displaying the correct table name in the UI can still fail if its underlying `linkedServiceName` reference points to a different (possibly misconfigured or differently-named) Linked Service than expected. Always verify the *full* JSON chain, not just what the UI surface shows.

3. **Dynamic content in trigger parameters isn't always exposed cleanly in the UI** — some ADF Studio versions don't render the "Add dynamic content" (⚡) helper consistently for trigger parameter panels, requiring direct JSON editing as a reliable fallback.

4. **Azure Event Grid must be explicitly registered** as a resource provider before Storage Event Triggers will function — a subscription-level prerequisite that isn't obvious from the trigger creation UI itself.

5. **GitHub's branch comparison UI defaults unhelpfully** (`base: main` vs `compare: main`) rather than automatically selecting your actual feature branch — always explicitly set both sides of a comparison rather than trusting the default.

---

## Cost Management

- Both triggers (`trigger1` and `trg_tumbling_hourly`) should be **stopped** when not actively testing, to avoid unnecessary runs (Manage → Triggers → Stop).
- Azure SQL Database on a Serverless tier auto-pauses when idle; confirm this tier is in use to minimize idle cost.
- When the project is fully done being used/demoed, delete both resource groups entirely to stop all associated costs:
  ```bash
  az group delete --name rg-retail-incremental-pipeline
  az group delete --name rg-retail-incremental-pipeline-test
  ```
