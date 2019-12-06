import uuid
import unittest
from datetime import datetime

import listenbrainz_spark
import listenbrainz_spark.recommendations.create_dataframes as dataframe
from listenbrainz_spark import schema, utils, config, path, hdfs_connection

from pyspark.sql import Row

# path used in between test functions of this class
LISTENS_PATH = '/test/listens/' # HDFS path to listens
MAPPING_PATH = '/test/mapping.parquet'
MAPPED_LISTENS_PATH = '/test/mapped_listens.parquet'

DATE = datetime.utcnow()

class CreateDataframeTestclass(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        listenbrainz_spark.init_test_session('spark-test-run-{}'.format(str(uuid.uuid4())))
        hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
        cls.upload_test_listen_to_HDFS()
        cls.upload_test_mapping_to_HDFS()
        cls.upload_test_mapped_listens_to_HDFS()

    @classmethod
    def tearDownClass(cls):
        utils.delete_dir('/', recursive=True)
        listenbrainz_spark.context.stop()

    @classmethod
    def upload_test_listen_to_HDFS(cls):
        month, year = DATE.strftime('%m'), DATE.strftime('%Y')

        test_listen = {
            "user_name": "vansika", "artist_msid": "a36d6fc9-49d0-4789-a7dd-a2b72369ca45",
            "artist_name": "Less Than Jake", "artist_mbids": [], "release_mbid": "", "track_name": "Al's War",
            "recording_msid": "cb6985cd-cc71-4d59-b4fb-2e72796af741", "tags": [], "listened_at": DATE
        }

        test_listens_df = utils.create_dataframe(schema.convert_to_spark_json(test_listen), schema.listen_schema)
        utils.save_parquet(test_listens_df, LISTENS_PATH + '{}/{}.parquet'.format(year, month))

    @classmethod
    def upload_test_mapping_to_HDFS(cls):
        test_mapping = {"msb_recording_msid":"cb6985cd-cc71-4d59-b4fb-2e72796af741"
            ,"mb_recording_gid":"3acb406f-c716-45f8-a8bd-96ca3939c2e5","msb_artist_msid":"a36d6fc9-49d0-4789-a7dd-a2b72369ca45",
            "mb_artist_gids":["181c4177-f33a-441d-b15d-910acaf18b07"],"mb_artist_credit_id":2157963}

        test_mapping_df = utils.create_dataframe(schema.convert_mapping_to_row(test_mapping), schema.mapping_schema)
        utils.save_parquet(test_mapping_df, MAPPING_PATH)

    @classmethod
    def upload_test_mapped_listens_to_HDFS(cls):
        partial_listen_df = dataframe.get_listens_for_training_model_window(DATE, DATE, {}, LISTENS_PATH)
        mapping_df = utils.read_files_from_HDFS(MAPPING_PATH)

        mapped_df = dataframe.get_mapped_artist_and_recording_mbids(partial_listen_df, mapping_df)
        utils.save_parquet(mapped_df, MAPPED_LISTENS_PATH)

    def test_get_dates_to_train_data(self):
        to_date, from_date = dataframe.get_dates_to_train_data()
        self.assertLessEqual(from_date, to_date)

    def test_get_listens_for_training_model_window(self):
        metadata = {}
        test_df = dataframe.get_listens_for_training_model_window(DATE, DATE, metadata, LISTENS_PATH)
        self.assertEqual(metadata['to_date'], DATE)
        self.assertEqual(metadata['from_date'], DATE)
        self.assertNotIn(['artist_mbids', 'recording_mbid'], test_df.columns)

    def test_save_dataframe(self):
        path_ = '/test_df.parquet'
        df = utils.create_dataframe(Row(column1=1, column2=2), schema=None)
        dataframe.save_dataframe(df, path_)

        status = utils.get_status(path_)
        self.assertTrue(status)

    def test_get_mapped_artist_and_recording_mbids(self):
        partial_listen_df = dataframe.get_listens_for_training_model_window(DATE, DATE, {}, LISTENS_PATH)
        mapping_df = utils.read_files_from_HDFS(MAPPING_PATH)

        mapped_df = dataframe.get_mapped_artist_and_recording_mbids(partial_listen_df, mapping_df)
        self.assertEqual(mapped_df.count(), 1)
        complete_listen_col = ['artist_msid', 'artist_name', 'listened_at', 'recording_msid', 'release_mbid', 'release_msid',
            'release_name', 'tags', 'track_name', 'user_name', 'mb_artist_credit_id', 'mb_artist_gids', 'mb_recording_gid',
             'msb_artist_msid', 'msb_recording_msid']
        self.assertEqual(complete_listen_col, mapped_df.columns)
        status = utils.get_status(path.MAPPED_LISTENS)
        self.assertTrue(status)

    def test_get_users_dataframe(self):
        metadata = {}
        mapped_df = utils.read_files_from_HDFS(MAPPED_LISTENS_PATH)
        users_df = dataframe.get_users_dataframe(mapped_df, metadata)
        self.assertEqual(users_df.count(), 1)
        self.assertEqual(['user_name', 'user_id'], users_df.columns)
        self.assertEqual(metadata['users_count'], users_df.count())

        status = utils.get_status(path.USERS_DATAFRAME_PATH)
        self.assertTrue(status)

    def test_get_recordings_dataframe(self):
        metadata = {}
        mapped_df = utils.read_files_from_HDFS(MAPPED_LISTENS_PATH)
        recordings_df = dataframe.get_recordings_df(mapped_df, metadata)
        self.assertEqual(recordings_df.count(), 1)
        self.assertEqual(['mb_recording_gid', 'mb_artist_credit_id', 'recording_id'], recordings_df.columns)
        self.assertEqual(metadata['recordings_count'], 1)

        status = utils.get_status(path.RECORDINGS_DATAFRAME_PATH)
        self.assertTrue(status)

    def test_get_listens_df(self):
        metadata = {}
        mapped_df = utils.read_files_from_HDFS(MAPPED_LISTENS_PATH)
        listens_df = dataframe.get_listens_df(mapped_df, metadata)
        self.assertEqual(listens_df.count(), 1)
        self.assertEqual(['mb_recording_gid', 'user_name'], listens_df.columns)
        self.assertEqual(metadata['listens_count'], 1)

    def test_get_playcounts_df(self):
        metadata = {}
        mapped_df = utils.read_files_from_HDFS(MAPPED_LISTENS_PATH)
        users_df = dataframe.get_users_dataframe(mapped_df, {})
        recordings_df = dataframe.get_recordings_df(mapped_df, {})
        listens_df = dataframe.get_listens_df(mapped_df, {})

        playcounts_df = dataframe.get_playcounts_df(listens_df, recordings_df, users_df, metadata)
        self.assertEqual(playcounts_df.count(), 1)
        self.assertEqual(['user_id', 'recording_id', 'count'], playcounts_df.columns)
        self.assertEqual(metadata['playcounts_count'], playcounts_df.count())

        status = utils.get_status(path.PLAYCOUNTS_DATAFRAME_PATH)
        self.assertTrue(status)

    def test_generate_best_model_id(self):
        metadata = {}
        dataframe.generate_best_model_id(metadata)
        self.assertTrue(metadata['model_id'])

    def test_save_dataframe_metadata_to_HDFS(self):
        metadata = {
            'from_date': DATE, 'to_date': DATE, 'listens_count': 1, 'model_id': '1', 'playcounts_count': 1,
            'recordings_count': 1, 'updated': True, 'users_count': 1
        }
        dataframe.save_dataframe_metadata_to_HDFS(metadata)
        status = utils.get_status(path.MODEL_METADATA)
        self.assertTrue(status)
