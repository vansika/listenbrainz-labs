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
LISTENS_PATH = '/test/listens/' # HDFS path to listens

END_DATE = datetime.utcnow().replace(microsecond=0).replace(day=1) # For listens of the current month
START_DATE = (END_DATE + relativedelta(months=-1)) # For listens of the previous month.

class GenerateTestData(SparkTestCase):

    def create_json_files(self, filename, date):
        """ Create JSON files of valid ListenBrainz listens i.e add timestamp.

            Args:
                filename (str): File containing listens without timestamp.
                date (datetime): Date to reference month for which timestamp should be generated.
        """
        file = open(TESTDATA_PATH + '{}.json'.format(date.strftime('%m')), 'w') # JSON file to save.
        with open(filename) as f:
            d1 = date # the first date of the month passed as argument
            d2 = (d1 + relativedelta(months=1)).replace(day=1) # the first date of the next month.
            for data in f:
              listen = json.loads(data)
              # We don't want to generate timestamp of any month other than the month of the date passed as argument.
              if d1 == d2:
                d1 = date
              listen['listened_at'] = d1.isoformat() + 'Z'
              json.dump(listen, file)
              file.write('\n')
              d1 += relativedelta(days=1)
        file.close()

    def generate_timestamps_for_listens(self):
        """ Generate initial timestamp for JSON files.
        """
        self.create_json_files(TESTDATA_PATH + 'test_listens_copy1.json', START_DATE)
        self.create_json_files(TESTDATA_PATH + 'test_listens_copy2.json', END_DATE)

    def upload_listens_to_hdfs(self, month, year):
        """ Upload test listens to HDFS.

            Args:
                temp_hdfs_path (str): Path where listens are stored in HDFS temporarily.
                month (str): month to which the listens belong.
                year (str): year to which listens belong.
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

        temp_dir = tempfile.mkdtemp()
        temp_hdfs_path = os.path.join(temp_dir, '{}.json'.format(month))
        hdfs_connection.client.upload(hdfs_path=temp_hdfs_path, local_path=TESTDATA_PATH + '{}.json'.format(month))
        file = listenbrainz_spark.session.read.json(HDFS_CLUSTER_URI + temp_hdfs_path, schema=listen_schema)
        file.write.format('parquet').save(HDFS_CLUSTER_URI + LISTENS_PATH + '{}/{}.parquet'.format(year, month))

    def test_prepare_listens(self):
        """ Invoke functions to create and upload test listens.
        """
        self.generate_timestamps_for_listens()
        m1, m2 = START_DATE.strftime('%m'), END_DATE.strftime('%m')
        self.upload_listens_to_hdfs(m1, START_DATE.strftime('%Y'))
        self.upload_listens_to_hdfs(m2, END_DATE.strftime('%Y'))
        # Remove the JSON files from local directory after listens have been uploaded to HDFS.
        os.remove(TESTDATA_PATH + '{}.json'.format(m1))
        os.remove(TESTDATA_PATH + '{}.json'.format(m2))



