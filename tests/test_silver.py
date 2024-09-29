# Databricks notebook source
from datetime import datetime
from pyspark.sql import DataFrame

def build_file_path(storage_account_name, container_name, blob_name):
    return f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{blob_name}"

def read_json_to_df(file_path):
    return spark.read.json(file_path)

def write_df_to_parquet(df, output_path):
    df.write.mode("overwrite").partitionBy("country", "state").parquet(output_path)
    print("Data successfully written to Azure Blob Storage in Parquet format.")

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession

class TestAzureBlobOperations(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("UnitTest").getOrCreate()
        self.storage_account_name = "desafioabinbev"
        self.container_name_bronze = "abinbev-bronze"
        self.container_name_silver = "abinbev-silver"
        self.today = datetime.today().strftime('%Y%m%d')
        self.blob_name = f"{self.today}/breweries_data.json"
        self.file_path_bronze = build_file_path(self.storage_account_name, self.container_name_bronze, self.blob_name)
        self.output_path_silver = build_file_path(self.storage_account_name, self.container_name_silver, self.today)

    def test_read_json_to_df(self):
        df = read_json_to_df(self.file_path_bronze)
        self.assertIsNotNone(df)
        self.assertGreater(df.count(), 0, "DataFrame should not be empty")

    def test_write_df_to_parquet(self):
        df = read_json_to_df(self.file_path_bronze)
        write_df_to_parquet(df, self.output_path_silver)
        # Check if the data was written successfully
        written_df = self.spark.read.parquet(self.output_path_silver)
        self.assertIsNotNone(written_df)
        self.assertGreater(written_df.count(), 0, "Written DataFrame should not be empty")

if __name__ == '__main__':
    unittest.main(argv=[""], exit=False)