import os
import unittest
from datetime import datetime

import listenbrainz_spark
from listenbrainz_spark import utils, hdfs_connection
from listenbrainz_spark.tests import SparkTestCase

from pyspark.sql import Row

HDFS_CLUSTER_URI = 'hdfs://hadoop-master:9000'

class UtilsTestCase(SparkTestCase):

    def test_append_dataframe(self):
        path_ = 'test_df.parquet'
        hdfs_path = os.path.join(HDFS_CLUSTER_URI, path_)

        df = utils.create_dataframe(Row(column1=1, column2=2), schema=None)
        utils.append(df, hdfs_path)
        new_df = utils.read_files_from_HDFS(hdfs_path)
        self.assertEqual(new_df.count(), 1)

        df = utils.create_dataframe(Row(column1=3, column2=4), schema=None)
        utils.append(df, hdfs_path)
        appended_df = utils.read_files_from_HDFS(hdfs_path)
        self.assertEqual(appended_df.count(), 2)
        # delete_dir dir in every test func so that using identical dirs by chance will not throw error.
        # remember not to prepend HDFS_CLUSTER_URI to a path when using HDFS client.
        utils.delete_dir('/' + path_, recursive=True)

    def test_create_dataframe(self):
        path_ = 'test_df.parquet'
        hdfs_path = os.path.join(HDFS_CLUSTER_URI, path_)

        df = utils.create_dataframe(Row(column1=1, column2=2), schema=None)
        self.assertEqual(df.count(), 1)
        utils.save_parquet(df, hdfs_path)

        received_df = utils.read_files_from_HDFS(hdfs_path)
        self.assertEqual(received_df.count(), 1)
        utils.delete_dir('/' + path_, recursive=True)

    def test_create_dir(self):
        path_ = '/tests/test'
        utils.create_dir(path_)
        status = utils.get_status(path_)
        self.assertEqual(status, True)
        utils.delete_dir(path_, recursive=True)

    def test_delete_dir(self):
        path_ = '/tests/test'
        utils.create_dir(path_)

        utils.delete_dir(path_)
        status = utils.get_status(path_)
        self.assertEqual(status, False)

    def test_get_listens(self):
        from_date = datetime(2019, 10, 1)
        to_date = datetime(2019, 11, 1)
        path_ = 'test_df'
        hdfs_path = os.path.join(HDFS_CLUSTER_URI, path_)

        df = utils.create_dataframe(Row(column1=1, column2=2), schema=None)
        dest_path = hdfs_path + '/{}/{}.parquet'.format(from_date.year, from_date.month)
        utils.save_parquet(df, dest_path)

        df = utils.create_dataframe(Row(column1=3, column2=4), schema=None)
        dest_path = hdfs_path + '/{}/{}.parquet'.format(to_date.year, to_date.month)
        utils.save_parquet(df, dest_path)

        received_df = utils.get_listens(from_date, to_date, hdfs_path)
        self.assertEqual(received_df.count(), 2)
        utils.delete_dir('/' + path_, recursive=True)

    def test_get_status(self):
        path_ = '/tests/test'
        utils.create_dir(path_)

        status = utils.get_status(path_)
        self.assertEqual(status, True)
        utils.delete_dir(path_)
        status = utils.get_status(path_)
        self.assertEqual(status, False)

    def test_save_parquet(self):
        path_ = 'test_df.parquet'
        hdfs_path = os.path.join(HDFS_CLUSTER_URI, path_)

        df = utils.create_dataframe(Row(column1=1, column2=2), schema=None)
        utils.save_parquet(df, hdfs_path)

        received_df = utils.read_files_from_HDFS(hdfs_path)
        self.assertEqual(received_df.count(), 1)
        utils.delete_dir('/' + path_, recursive=True)
