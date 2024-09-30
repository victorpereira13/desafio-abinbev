# Databricks notebook source
import requests
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import unittest
from unittest.mock import patch, MagicMock

# COMMAND ----------

# MAGIC %run ./Bronze

# COMMAND ----------

api_url = "https://api.openbrewerydb.org/v1/breweries"

class TestBronzeFunctions(unittest.TestCase):
    def test_get_today_date(self):
        today = datetime.today().strftime("%Y%m%d")
        self.assertEqual(get_today_date(), today)

    def test_get_blob_name(self):
        today = datetime.today().strftime("%Y%m%d")
        expected_blob_name = f"{today}/breweries_data.json"
        self.assertEqual(get_blob_name(), expected_blob_name)

    @patch("requests.get")
    def test_fetch_data_from_api(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": 1, "name": "Test Brewery"}]
        mock_get.return_value = mock_response

        data = fetch_data_from_api(api_url)
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        self.assertEqual(data[0]["name"], "Test Brewery")

    def test_convert_data_to_json(self):
        sample_data = [{"id": 1, "name": "Test Brewery"}]
        try:
            json_data = convert_data_to_json(sample_data)
            self.assertIsInstance(json_data, str)
        except (TypeError, ValueError) as e:
            self.fail(f"Error converting data to JSON: {e}")

if __name__ == "__main__":
    unittest.main(argv=[""], verbosity=2, exit=False)