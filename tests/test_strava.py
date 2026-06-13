import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from botocore.exceptions import ClientError

from scripts.strava_importer import (
    PipelineConfig,
    ActivityTransformer,
    StravaExtractor,
    StravaETLPipeline
)

class TestActivityTransformer(unittest.TestCase):
    def setUp(self):
        self.config = PipelineConfig(
            strava_client_id="dummy_id",
            strava_client_secret="dummy_secret",
            strava_refresh_token="dummy_token",
            strava_club_id=123,
            strava_fetch_pages=1,
            s3_bucket_name="dummy_bucket",
            s3_key="dummy_key"
        )
        self.transformer = ActivityTransformer(self.config)

    def test_process_raw_activities_empty(self):
        df = self.transformer.process_raw_activities([], 1)
        self.assertTrue(df.empty)

    def test_process_raw_activities_calculations(self):
        sample_raw = [{
            "distance": 1000,
            "moving_time": 600,
            "elapsed_time": 700,
            "athlete": {"firstname": "Test", "lastname": "User"},
            "name": "Morning Run",
            "sport_type": "Run",
            "start_date": "2024-04-11T06:00:00Z"
        }]
        df = self.transformer.process_raw_activities(sample_raw, 1)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["distance_m"], 1000)
        self.assertEqual(df.iloc[0]["distance_km"], 1.0)
        self.assertEqual(df.iloc[0]["moving_time_min"], 10.0)
        self.assertEqual(df.iloc[0]["elapsed_time_min"], 700 / 60.0)
        self.assertEqual(df.iloc[0]["pace_min_per_km"], 10.0)

    def test_deduplicate_and_merge(self):
        df_old = pd.DataFrame([{
            "athlete_firstname": "Test",
            "athlete_lastname": "User",
            "name": "Morning Run",
            "sport_type": "Run",
            "distance_m": 1000,
            "moving_time_s": 600,
            "elapsed_time_s": 700,
            "total_elevation_gain": 10,
            "snapshot_time": "2024-04-11 05:00:00",
            "Batch": 1,
            "kudos_count": 0
        }])
        
        df_new = pd.DataFrame([{
            "athlete_firstname": "Test",
            "athlete_lastname": "User",
            "name": "Morning Run",
            "sport_type": "Run",
            "distance_m": 1000,
            "moving_time_s": 600,
            "elapsed_time_s": 700,
            "total_elevation_gain": 10,
            "snapshot_time": "2024-04-11 06:00:00",
            "Batch": 2,
            "kudos_count": 5  # Kudos updated!
        }])

        merged = self.transformer.deduplicate_and_merge(df_old, df_new)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged.iloc[0]["kudos_count"], 5)  # Keeps latest metrics
        self.assertEqual(merged.iloc[0]["Batch"], 1)        # Retains earliest batch ID
        self.assertEqual(merged.iloc[0]["snapshot_time"], "2024-04-11 05:00:00")  # Retains earliest ingestion time


class TestStravaExtractor(unittest.TestCase):
    def setUp(self):
        self.config = PipelineConfig(
            strava_client_id="dummy_id",
            strava_client_secret="dummy_secret",
            strava_refresh_token="dummy_token",
            strava_club_id=123,
            strava_fetch_pages=1,
            s3_bucket_name="dummy_bucket",
            s3_key="dummy_key"
        )
        self.mock_s3 = MagicMock()
        self.mock_session = MagicMock()
        self.extractor = StravaExtractor(self.config, s3_client=self.mock_s3, session=self.mock_session)

    def test_get_access_token(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "valid_token"}
        self.mock_session.post.return_value = mock_response

        token = self.extractor.get_access_token()
        self.assertEqual(token, "valid_token")
        self.mock_session.post.assert_called_once()

    def test_download_excel_from_s3_missing(self):
        # Setup mock S3 client to raise NoSuchKey ClientError
        error_response = {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}}
        self.mock_s3.get_object.side_effect = ClientError(error_response, "GetObject")

        df = self.extractor.download_excel_from_s3()
        self.assertIsNone(df)


if __name__ == "__main__":
    unittest.main()
