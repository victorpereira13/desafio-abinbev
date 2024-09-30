# Databricks notebook source
# MAGIC %run ./Gold

# COMMAND ----------

import unittest
from pyspark.sql import Row
from datetime import datetime

class TestDataProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()
        cls.storage_account_name = "desafioabinbev"
        cls.container_name_silver = "abinbev-silver"
        cls.today = datetime.today().strftime("%Y%m%d")
        cls.base_path_silver = build_file_path(cls.storage_account_name, cls.container_name_silver, "")
        cls.file_path_silver = build_file_path(cls.storage_account_name, cls.container_name_silver, f"{cls.today}")

    def test_read_parquet_to_df(self):
        # Create a sample DataFrame
        data = [
            Row(country="USA", state="CA", brewery_type="micro"),
            Row(country="USA", state="NY", brewery_type="nano"),
        ]
        df = self.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(self.file_path_silver)

        # Read the DataFrame
        result_df = read_parquet_to_df(self.file_path_silver, self.base_path_silver)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 2)

    def test_validate_data(self):
        valid_data = [
            Row(id="1", country="United States", state="California", brewery_type="micro"),
            Row(id="2", country="United States", state="New York", brewery_type="nano")
        ]
        valid_df = self.spark.createDataFrame(valid_data)
        self.assertTrue(validate_data(valid_df))

        invalid_data = [
            Row(id=None, country="United States", state="California", brewery_type="micro"),
            Row(id="2", country="United States", state="New York", brewery_type="nano")
        ]
        invalid_df = self.spark.createDataFrame(invalid_data)
        self.assertFalse(validate_data(invalid_df))

    def test_aggregate_breweries(self):
        # Create a sample DataFrame
        data = [
            Row(country="United States", state="California", brewery_type="micro"),
            Row(country="United States", state="California", brewery_type="micro"),
            Row(country="United States", state="New York", brewery_type="nano"),
        ]
        df = self.spark.createDataFrame(data)

        # Aggregate the DataFrame
        aggregated_df = aggregate_breweries(df)
        self.assertEqual(aggregated_df.count(), 2)
        self.assertEqual(
            aggregated_df.filter(
                "country = 'United States' AND state = 'California' AND brewery_type = 'micro'"
            ).collect()[0]["quantity"],
            2,
        )

    def test_save_aggregated_df(self):
        # Create a sample aggregated DataFrame
        data = [
            Row(
                country="United States",
                state="California",
                brewery_type="micro",
                quantity=2,
            ),
            Row(
                country="United States",
                state="New York",
                brewery_type="nano",
                quantity=1,
            ),
        ]
        aggregated_df = self.spark.createDataFrame(data)

        # Save the DataFrame
        save_aggregated_df(aggregated_df, "desafio_abinbev.view.brewery_aggregated")

        # Read the saved table
        result_df = self.spark.table("desafio_abinbev.view.brewery_aggregated")
        self.assertEqual(result_df.count(), 2)

if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)