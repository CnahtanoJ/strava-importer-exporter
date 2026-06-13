# Strava Importer & Exporter

A consolidated repository for automating Strava club data fetching (to S3) and exporting consolidated data (to Dropbox).

## Project Structure

- `scripts/strava_importer.py`: Orchestrates the ETL pipeline that fetches data from the Strava API, processes and cleans the data, merges it with historical S3 records, and saves the updated state back to S3.
- `scripts/strava_exporter.py`: Orchestrates the export pipeline, downloading the processed spreadsheet from S3 and uploading it to Dropbox.
- `tests/test_strava.py`: Unit test suite testing data transformations and mocking API/AWS interactions.

## Architecture & Design Principles

This codebase is built using professional Software Engineering and Data Engineering best practices:

1. **Modular ETL Architecture**: Rather than scripting line-by-line procedural code, the pipeline is split into explicit classes representing standard ETL phases:
   - **Extractor (`StravaExtractor` / `S3Downloader`)**: Interacts with raw external interfaces (Strava HTTP API / AWS S3 API).
   - **Transformer (`ActivityTransformer`)**: Pure business logic class that processes dataframes, computes derived metrics, and performs idempotent deduplication.
   - **Loader (`S3Loader` / `DropboxUploader`)**: Handles output state persistence.
   - **Orchestrator (`StravaETLPipeline` / `DropboxExportPipeline`)**: Coordinates the sequential flow of data between stages.
2. **Dependency Injection**: Network sessions (`requests.Session`) and AWS clients (`boto3.client`) are injected during construction rather than instantiated globally. This enables easy mocking during unit testing and supports alternative HTTP configurations or AWS session scopes.
3. **Config-Driven Development**: Configurations are consolidated into type-hinted Dataclasses (`PipelineConfig` and `ExporterConfig`), validating settings before runtime and avoiding direct environment calls in the middle of core logic.
4. **Idempotence & State Preservation**: When merging new batches of data, the pipeline ensures duplicate entries are resolved by keeping the latest update of metrics (e.g. kudos, comments) while retaining the earliest ingestion timestamp (`snapshot_time`) and batch number to preserve historical pipeline lineage.
5. **Robust Error Handling & Standard Logging**: Replaced print statements with standard Python `logging`. Handled S3 state gracefully (such as initializing baseline data on S3 `NoSuchKey` errors).

## Local Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Secrets**:
   - Copy `.env.example` to `.env`.
   - Fill in your Strava, AWS, and Dropbox credentials.

3. **Run Locally**:
   ```bash
   # Run the importer
   python scripts/strava_importer.py

   # Run the exporter
   python scripts/strava_exporter.py
   ```

## Testing

The project uses `unittest` with mock interfaces to verify pipeline logic without relying on external network requests or active AWS accounts.

Run tests using:
```bash
python -m unittest discover -s tests
```

## AWS Lambda Deployment

### 1. Requirements & Layers
Since `pandas` and `numpy` are large, you should deploy them using a **Lambda Layer**. 
- Recommended: Use [Klayers](https://github.com/keithrozario/Klayers) for pre-built layers.
- Or use a Docker container if the size exceeds 250MB.

### 2. Environment Variables
Ensure the following variables are set in the Lambda Configuration:
- `STRAVA_CLIENT_ID`
- `STRAVA_CLIENT_SECRET`
- `STRAVA_REFRESH_TOKEN`
- `S3_BUCKET_NAME`
- `S3_KEY`
- `DROPBOX_REFRESH_TOKEN`
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_DEST_PATH`

## Security Warning
**Never commit your `.env` file!** It is already excluded in `.gitignore`.

---

## Optional: Downstream Integrations (No-Code)

Since the consolidated data is exported to **Dropbox**, you can easily trigger further actions using **Power Automate**, **Zapier**, or **IFTTT**:

- **Email Notifications**: Trigger an email via Outlook/Gmail whenever a new `club_export.xlsx` version is uploaded to Dropbox.
- **Teams/Slack Alerts**: Post a summary or a link to the new file in a specific channel.
- **Excel Online Sync**: Use Power Automate to copy the data from the Dropbox file into an Excel Online table for live dashboarding.
