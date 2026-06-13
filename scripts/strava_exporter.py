import os
import json
import logging
from dataclasses import dataclass
from typing import Dict, Any

import boto3
import requests
from requests.auth import HTTPBasicAuth

# Configure logging
logger = logging.getLogger(__name__)

# Use dotenv for local testing if needed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

@dataclass(frozen=True)
class ExporterConfig:
    """Configuration class for the Dropbox Exporter pipeline."""
    s3_bucket: str
    s3_key: str
    dropbox_refresh_token: str
    dropbox_app_key: str
    dropbox_app_secret: str
    dropbox_dest_path: str

    @classmethod
    def from_env(cls) -> "ExporterConfig":
        """Load and validate configuration from environment variables."""
        s3_bucket = os.environ.get("S3_BUCKET_NAME")
        s3_key = os.environ.get("S3_KEY")
        db_refresh = os.environ.get("DROPBOX_REFRESH_TOKEN")
        db_key = os.environ.get("DROPBOX_APP_KEY")
        db_secret = os.environ.get("DROPBOX_APP_SECRET")
        db_path = os.environ.get("DROPBOX_DEST_PATH")

        if not all([s3_bucket, s3_key, db_refresh, db_key, db_secret, db_path]):
            raise EnvironmentError("Missing required environment variables for Dropbox export.")

        return cls(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            dropbox_refresh_token=db_refresh,
            dropbox_app_key=db_key,
            dropbox_app_secret=db_secret,
            dropbox_dest_path=db_path
        )


class S3Downloader:
    """Handles downloading file content from AWS S3."""

    def __init__(self, config: ExporterConfig, s3_client=None):
        self.config = config
        self.s3 = s3_client or boto3.client("s3")

    def download(self) -> bytes:
        """Download the target file content from S3."""
        logger.info("Downloading file from S3: s3://%s/%s", self.config.s3_bucket, self.config.s3_key)
        response = self.s3.get_object(Bucket=self.config.s3_bucket, Key=self.config.s3_key)
        return response["Body"].read()


class DropboxUploader:
    """Handles refreshing Dropbox OAuth tokens and uploading files to Dropbox."""

    TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
    UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"

    def __init__(self, config: ExporterConfig, session=None):
        self.config = config
        self.session = session or requests.Session()

    def get_access_token(self) -> str:
        """Refresh the Dropbox access token using the refresh token."""
        logger.info("Refreshing Dropbox access token...")
        response = self.session.post(
            self.TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.config.dropbox_refresh_token,
            },
            auth=HTTPBasicAuth(self.config.dropbox_app_key, self.config.dropbox_app_secret),
            timeout=30
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def upload(self, file_content: bytes, access_token: str) -> None:
        """Upload the file content to Dropbox."""
        logger.info("Uploading file content to Dropbox path: %s", self.config.dropbox_dest_path)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Dropbox-API-Arg": json.dumps({
                "path": self.config.dropbox_dest_path,
                "mode": "overwrite",
                "autorename": False,
                "mute": False,
                "strict_conflict": False
            }),
            "Content-Type": "application/octet-stream"
        }

        response = self.session.post(
            self.UPLOAD_URL,
            headers=headers,
            data=file_content,
            timeout=60
        )
        response.raise_for_status()
        logger.info("Upload to Dropbox completed successfully.")


class DropboxExportPipeline:
    """Orchestrates the export pipeline, copying the data from S3 to Dropbox."""

    def __init__(self, config: ExporterConfig, s3_client=None, session=None):
        self.config = config
        self.downloader = S3Downloader(config, s3_client=s3_client)
        self.uploader = DropboxUploader(config, session=session)

    def run(self) -> Dict[str, Any]:
        """Run the end-to-end export pipeline."""
        logger.info("Starting Dropbox export pipeline.")
        file_content = self.downloader.download()
        access_token = self.uploader.get_access_token()
        self.uploader.upload(file_content, access_token)
        return {
            "statusCode": 200,
            "body": f"File successfully exported to Dropbox at {self.config.dropbox_dest_path}"
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point."""
    try:
        config = ExporterConfig.from_env()
        pipeline = DropboxExportPipeline(config)
        return pipeline.run()
    except Exception as e:
        logger.exception("Dropbox export pipeline failed:")
        return {
            "statusCode": 500,
            "body": str(e)
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        pipeline_config = ExporterConfig.from_env()
        pipeline = DropboxExportPipeline(pipeline_config)
        pipeline.run()
    except Exception as err:
        logger.exception("Pipeline execution failed:")
