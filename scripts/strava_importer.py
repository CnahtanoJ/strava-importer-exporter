import os
import logging
from dataclasses import dataclass
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import boto3
import requests
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger(__name__)

# Use dotenv for local testing if needed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

@dataclass(frozen=True)
class PipelineConfig:
    """Configuration class for the Strava Importer pipeline."""
    strava_client_id: str
    strava_client_secret: str
    strava_refresh_token: str
    strava_club_id: int
    strava_fetch_pages: int
    s3_bucket_name: str
    s3_key: str

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Load and validate configuration from environment variables."""
        client_id = os.environ.get("STRAVA_CLIENT_ID")
        client_secret = os.environ.get("STRAVA_CLIENT_SECRET")
        refresh_token = os.environ.get("STRAVA_REFRESH_TOKEN")
        
        if not all([client_id, client_secret, refresh_token]):
            raise EnvironmentError("Missing required Strava credentials in environment variables.")

        return cls(
            strava_client_id=client_id,
            strava_client_secret=client_secret,
            strava_refresh_token=refresh_token,
            strava_club_id=int(os.environ.get("STRAVA_CLUB_ID", 1624555)),
            strava_fetch_pages=int(os.environ.get("STRAVA_FETCH_PAGES", 1)),
            s3_bucket_name=os.environ.get("S3_BUCKET_NAME", "flaminghotcheetos"),
            s3_key=os.environ.get("S3_KEY", "club_export.xlsx")
        )


class StravaExtractor:
    """Handles extracting data from Strava API and reading baseline state from S3."""
    
    STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
    STRAVA_API_BASE = "https://www.strava.com/api/v3"

    def __init__(self, config: PipelineConfig, s3_client=None, session=None):
        self.config = config
        self.s3 = s3_client or boto3.client("s3")
        self.session = session or requests.Session()

    def get_access_token(self) -> str:
        """Use the stored refresh token to obtain a short-lived access token."""
        payload = {
            "client_id": self.config.strava_client_id,
            "client_secret": self.config.strava_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.config.strava_refresh_token,
        }
        resp = self.session.post(self.STRAVA_TOKEN_URL, data=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()["access_token"]

    def fetch_club_activities(self, access_token: str, per_page: int = 200) -> List[Dict[str, Any]]:
        """Fetch recent activities from the configured Strava club feed."""
        headers = {"Authorization": f"Bearer {access_token}"}
        all_activities = []
        
        for page in range(1, self.config.strava_fetch_pages + 1):
            url = f"{self.STRAVA_API_BASE}/clubs/{self.config.strava_club_id}/activities"
            params = {"page": page, "per_page": per_page}
            resp = self.session.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_activities.extend(data)
            
        return all_activities

    def download_excel_from_s3(self) -> Optional[pd.DataFrame]:
        """Download and read the existing Excel baseline from S3."""
        try:
            response = self.s3.get_object(Bucket=self.config.s3_bucket_name, Key=self.config.s3_key)
            return pd.read_excel(BytesIO(response["Body"].read()), engine="openpyxl")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info("Baseline S3 file not found. Starting fresh.")
                return None
            logger.error("Failed to read baseline from S3: %s", e)
            raise e


class ActivityTransformer:
    """Handles structuring, cleaning, metric calculation, and deduplication of activity data."""

    def __init__(self, config: PipelineConfig):
        self.config = config

    @staticmethod
    def _get_snapshot_timestamp() -> str:
        """Return the current timestamp in Asia/Jakarta timezone."""
        tz = timezone(timedelta(hours=7))
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    def get_next_batch_number(self, df_old: Optional[pd.DataFrame]) -> int:
        """Determine the next sequential batch ID."""
        if df_old is None or df_old.empty or "Batch" not in df_old.columns:
            return 1
        try:
            return int(df_old["Batch"].max() + 1)
        except Exception:
            return 1

    def process_raw_activities(self, raw: List[Dict[str, Any]], batch_num: int) -> pd.DataFrame:
        """Flatten raw JSON activity records and compute derived pipeline metrics."""
        if not raw:
            return pd.DataFrame()

        df = pd.json_normalize(raw, sep="_")

        # Handle derived metrics safely
        df["distance_m"] = df.get("distance", 0)
        df["moving_time_s"] = df.get("moving_time", 0)
        df["elapsed_time_s"] = df.get("elapsed_time", 0)

        df["distance_km"] = df["distance_m"] / 1000.0
        df["moving_time_min"] = df["moving_time_s"] / 60.0
        df["elapsed_time_min"] = df["elapsed_time_s"] / 60.0

        # Calculate pace, handling division by zero or NaN safely
        df["pace_min_per_km"] = np.where(
            (df["distance_km"] == 0) | (df["distance_km"].isna()),
            np.nan,
            df["moving_time_min"] / df["distance_km"]
        )
        df["pace_min_per_km"] = df["pace_min_per_km"].round(5)

        df["snapshot_time"] = self._get_snapshot_timestamp()
        df["Batch"] = batch_num

        # Enforce column order for primary fields
        front_cols = [
            "athlete_firstname", "athlete_lastname", "athlete_id",
            "name", "sport_type", "start_date", "start_date_local", "distance_m",
            "distance_km", "moving_time_s", "moving_time_min", "elapsed_time_s", "elapsed_time_min",
            "pace_min_per_km", "total_elevation_gain", "average_speed", "max_speed",
            "average_heartrate", "max_heartrate", "kudos_count", "comment_count"
        ]
        front_cols = [col for col in front_cols if col in df.columns]
        remaining_cols = [col for col in df.columns if col not in front_cols]
        return df[front_cols + remaining_cols]

    def deduplicate_and_merge(self, df_old: Optional[pd.DataFrame], df_new: pd.DataFrame) -> pd.DataFrame:
        """Consolidate old and new datasets, resolving duplicates by retaining earliest metadata."""
        if df_old is None or df_old.empty:
            return df_new

        combined = pd.concat([df_old, df_new], ignore_index=True)
        combined = combined.sort_values("snapshot_time")

        # Core columns to uniquely identify an activity
        dup_cols = ["athlete_firstname", "athlete_lastname", "name", "sport_type",
                    "distance_m", "moving_time_s", "elapsed_time_s", "total_elevation_gain"]
        dup_cols = [col for col in dup_cols if col in combined.columns]

        # Calculate tracking info (retaining the earliest ingestion batch and timestamp)
        earliest_info = (
            combined.sort_values("snapshot_time")
            .groupby(dup_cols, as_index=False)
            .agg(
                earliest_snapshot_time=("snapshot_time", "first"),
                earliest_batch=("Batch", "first")
            )
        )

        # Drop duplicate records, keeping the most recent snapshot of other fields (kudos, comments, etc.)
        combined = combined.drop_duplicates(subset=dup_cols, keep="last")

        # Merge the baseline metadata back
        combined = combined.drop(columns=["snapshot_time", "Batch"], errors="ignore")
        combined = combined.merge(earliest_info, on=dup_cols, how="left")
        combined = combined.rename(columns={
            "earliest_snapshot_time": "snapshot_time",
            "earliest_batch": "Batch"
        })

        return combined


class S3Loader:
    """Handles loading final dataset back to Amazon S3."""

    def __init__(self, config: PipelineConfig, s3_client=None):
        self.config = config
        self.s3 = s3_client or boto3.client("s3")

    def upload_to_s3(self, df: pd.DataFrame) -> None:
        """Upload the pandas DataFrame back to S3 as an Excel spreadsheet."""
        buffer = BytesIO()
        df.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)
        self.s3.put_object(
            Bucket=self.config.s3_bucket_name,
            Key=self.config.s3_key,
            Body=buffer.getvalue()
        )


class StravaETLPipeline:
    """Orchestrator for the Strava ETL pipeline."""

    def __init__(self, config: PipelineConfig, s3_client=None, session=None):
        self.config = config
        self.extractor = StravaExtractor(config, s3_client=s3_client, session=session)
        self.transformer = ActivityTransformer(config)
        self.loader = S3Loader(config, s3_client=s3_client)

    def run(self) -> Dict[str, Any]:
        """Run the end-to-end pipeline."""
        logger.info("Initializing Strava ETL Pipeline run.")
        token = self.extractor.get_access_token()
        
        logger.info("Extracting raw club activities from Strava API.")
        raw_activities = self.extractor.fetch_club_activities(token)
        
        if not raw_activities:
            logger.info("No activities returned from API. Skipping merge and load.")
            return {"statusCode": 200, "body": "No new activities to process."}

        logger.info("Extracting baseline dataset from S3.")
        df_old = self.extractor.download_excel_from_s3()
        
        logger.info("Transforming extracted activities.")
        batch_num = self.transformer.get_next_batch_number(df_old)
        df_new = self.transformer.process_raw_activities(raw_activities, batch_num)
        
        logger.info("Merging and deduplicating data.")
        combined = self.transformer.deduplicate_and_merge(df_old, df_new)
        
        logger.info("Loading structured output back to S3.")
        self.loader.upload_to_s3(combined)
        
        logger.info("ETL pipeline execution succeeded. Appended batch %d.", batch_num)
        return {"statusCode": 200, "body": f"Successfully processed batch {batch_num}"}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point."""
    try:
        config = PipelineConfig.from_env()
        pipeline = StravaETLPipeline(config)
        return pipeline.run()
    except Exception as e:
        logger.exception("Pipeline failed:")
        return {"statusCode": 500, "body": str(e)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        pipeline_config = PipelineConfig.from_env()
        pipeline = StravaETLPipeline(pipeline_config)
        pipeline.run()
    except Exception as err:
        logger.exception("Pipeline execution failed:")
