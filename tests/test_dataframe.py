import os
import json
import tempfile
import unittest
from datetime import datetime
from dateutil.relativedelta import relativedelta

import listenbrainz_spark
from tests import SparkTestCase
from listenbrainz_spark import utils
from listenbrainz_spark import hdfs_connection

from pyspark.sql.types import StructField, StructType, ArrayType, StringType, TimestampType, FloatType, IntegerType, BooleanType

HDFS_CLUSTER_URI = 'hdfs://hadoop-master:9000' # the URI to be used with Spark
TESTDATA_PATH = '/rec/listenbrainz_spark/testdata/test_listens/' # local path to test listens
LISTENS_PATH = '/test/listens' # HDFS path to listens

class GenerateTestData(SparkTestCase):

    def create_json_files(self, filename, date):
        """ Create JSON files of valid ListenBrainz listens i.e add timestamp.

            Args:
                filename (str): File containing listens without timestamp.
                date (datetime): Date to reference month for which timestamp should be generated.
        """
        file = open(TESTDATA_PATH + '{}.json'.format(date.strftime('%m')), 'w') # JSON file to save.
        with open(filename) as f:
            start_date = date # the first date of the month
            end_date = (start_date + relativedelta(months=1)).replace(day=1) # the first date of the next month.
            for data in f:
              listen = json.loads(data)
              # We don't want to generate timestamp of any month other than the month of the date passed as argument.
              if start_date == end_date:
                start_date = date
              listen['listened_at'] = start_date.isoformat() + 'Z'
              json.dump(listen, file)
              file.write('\n')
              start_date += relativedelta(days=1)
        file.close()

    def generate_timestamps_for_listens(self):
        """ Generate initial timestamp for JSON files.

            Returns:
                m1 (str): Current month.
                m2 (str): Previous month.
        """
        end_date = datetime.utcnow().replace(microsecond=0).replace(day=1) # For listens of the current month
        start_date = (end_date + relativedelta(months=-1)) # For listens of the previous month.

        self.create_json_files(TESTDATA_PATH + 'test_listens_copy1.json', start_date)
        self.create_json_files(TESTDATA_PATH + 'test_listens_copy2.json', end_date)

        return start_date.strftime('%m'), end_date.strftime('%m')

    def upload_listens_to_hdfs(self, temp_hdfs_path, path):
        """ Upload test listens to HDFS.

            Args:
                temp_hdfs_path (str): Path where listens are stored in HDFS temporarily.
                path (str): local path of JSON files which contain test listens.
        """
        listen_schema = [
            StructField('artist_mbids', ArrayType(StringType()), nullable=True),
            StructField('artist_msid', StringType(), nullable=False),
            StructField('artist_name', StringType(), nullable=False),
            StructField('listened_at', TimestampType(), nullable=False),
            StructField('recording_mbid', StringType(), nullable=True),
            StructField('recording_msid', StringType(), nullable=False),
            StructField('release_mbid', StringType(), nullable=True),
            StructField('release_msid', StringType(), nullable=True),
            StructField('release_name', StringType(), nullable=True),
            StructField('tags', ArrayType(StringType()), nullable=True),
            StructField('track_name', StringType(), nullable=False),
            StructField('user_name', StringType(), nullable=False),
        ]

        listen_schema = StructType(sorted(listen_schema, key=lambda field: field.name))
        hdfs_connection.client.upload(hdfs_path=temp_hdfs_path, local_path=TESTDATA_PATH + path)
        file = listenbrainz_spark.session.read.json(HDFS_CLUSTER_URI + temp_hdfs_path, schema=listen_schema)
        file.write.format('parquet').save(config.HDFS_CLUSTER_URI + LISTENS_PATH + path)

    def test_prepare_listens(self):
        """ Function invoked by pytest. Invokes functions to create and upload test listens.
        """

        m1, m2 = self.generate_timestamps_for_listens()

        temp_dir = tempfile.mkdtemp()
        temp_hdfs_path = os.path.join(temp_dir, LISTENS_PATH)
        hdfs_connection.client.delete(LISTENS_PATH, recursive=True)
        self.upload_listens_to_hdfs(temp_hdfs_path, '{}.json'.format(m1))
        self.upload_listens_to_hdfs(temp_hdfs_path, '{}.json'.format(m2))
        hdfs_connection.client.delete(temp_dir, recursive=True)
        shutil.rmtree(temp_dir)
        # Remove the JSON files from local directory after listens have been uploaded to HDFS.
        os.remove(TESTDATA_PATH + '{}.json'.format(m1))
        os.remove(TESTDATA_PATH + '{}.json'.format(m1))



