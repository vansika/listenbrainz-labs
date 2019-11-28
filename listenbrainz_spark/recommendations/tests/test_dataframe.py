import unittest

from listenbrainz_spark import utils, hdfs_connection
from tests import SparkTestCase
from tests.test_generate_listens import LISTENS_PATH, HDFS_CLUSTER_URI, START_DATE, END_DATE

from pyspark.sql.functions import month
import pyspark.sql.functions as f

training_df = None

class CreateDataframesTestCases(SparkTestCase):

    def test_users_dataframe(self):
        training_df = utils.get_listens(START_DATE, END_DATE, HDFS_CLUSTER_URI + LISTENS_PATH)
        m1 = training_df.select(f.min('listened_at').alias('listened_at')).take(1)[0].listened_at.month
        m2 = training_df.select(f.max('listened_at').alias('listened_at')).take(1)[0].listened_at.month
        self.assertEqual(m1, int(START_DATE.strftime('%m')))
        self.assertEqual(m2, int(END_DATE.strftime('%m')))



