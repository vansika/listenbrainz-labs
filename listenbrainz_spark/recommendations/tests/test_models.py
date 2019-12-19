import uuid
import unittest

import listenbrainz_spark
from listenbrainz_spark.tests import SparkTestCase
from listenbrainz_spark import utils, config, hdfs_connection
import listenbrainz_spark.recommendations.train_models as model

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType

TEST_PLAYCOUNTS_PATH = '/tests/playcounts.parquet'
PLAYCOUNTS_COUNT = 100

class TrainModelsTestCase(SparkTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.upload_test_playcounts()

    @classmethod
    def tearDownClass(cls):
        super().delete_dir()
        super().tearDownClass()

    @classmethod
    def upload_test_playcounts(cls):
        schema = StructType(
            [
                StructField("user_id", IntegerType()),
                StructField("recording_id", IntegerType()),
                StructField("count", IntegerType())
            ]
        )
        test_playcounts = []
        for i in range(1, PLAYCOUNTS_COUNT + 1):
            test_playcounts.append([1, 1, 1])
        test_playcounts_df = utils.create_dataframe(test_playcounts, schema=schema)
        utils.save_parquet(test_playcounts_df, TEST_PLAYCOUNTS_PATH)

    def test_train(self):
        RANKS, LAMBDAS, ITERATIONS = [8], [0.1], [5]
        test_playcounts_df = utils.read_files_from_HDFS(TEST_PLAYCOUNTS_PATH)
        training_data, validation_data, test_data = model.preprocess_data(test_playcounts_df)
        best_model, model_metadata, best_model_metadata = model.train(training_data, validation_data,
            validation_data.count(), RANKS, LAMBDAS, ITERATIONS)
        self.assertTrue(best_model)
        self.assertEqual(len(model_metadata), 1)
        self.assertEqual(model_metadata[0][0], best_model_metadata['model_id'])
        self.assertEqual(model_metadata[0][1], best_model_metadata['training_time'])
        self.assertEqual(model_metadata[0][2], best_model_metadata['rank'])
        self.assertEqual(model_metadata[0][3], best_model_metadata['lmbda'])
        self.assertEqual(model_metadata[0][4], best_model_metadata['iteration'])
        self.assertEqual(model_metadata[0][5], best_model_metadata['error'])
        self.assertEqual(model_metadata[0][6], best_model_metadata['rmse_time'])

    def test_parse_dataset(self):
        row = Row(user_id=1, recording_id=1, count=1)
        rating_object = model.parse_dataset(row)
        self.assertEqual(rating_object.user, 1)
        self.assertEqual(rating_object.product, 1)
        self.assertEqual(rating_object.rating, 1)

    def test_preprocess_data(self):
        test_playcounts_df = utils.read_files_from_HDFS(TEST_PLAYCOUNTS_PATH)
        training_data, validation_data, test_data = model.preprocess_data(test_playcounts_df)
        total_playcounts = training_data.count() + validation_data.count() + test_data.count()
        self.assertEqual(total_playcounts, PLAYCOUNTS_COUNT)
