import utils
import sys
import os
import tempfile

from pyspark.mllib.recommendation import MatrixFactorizationModel
from pyspark import SparkContext
from listenbrainz_spark import hdfs_connection, config
from datetime import datetime
from pyspark.sql import SparkSession

spark = None
sc = None

def load_model(path):
    return MatrixFactorizationModel.load(sc, path)


def get_user_id(user_name):
    result = spark.sql("""
        SELECT user_id
          FROM user
         WHERE user_name = '%s'
    """ % user_name)
    return result.first()['user_id']


def recommend_user(user_name, model, recordings_map):
    user_id = get_user_id(user_name)
    user_playcounts = spark.sql("""
        SELECT user_id,
               recording_id,
               count
          FROM playcount
         WHERE user_id = %d
    """ % user_id)

    user_recordings = user_playcounts.rdd.map(lambda r: r['recording_id'])
    all_recordings =  recordings_map.keys()
    candidate_recordings = all_recordings.subtract(user_recordings)
    recommendations = model.predictAll(candidate_recordings.map(lambda recording: (user_id, recording))).takeOrdered(10, lambda product: -product.rating)

    recommended_recordings = [recordings_map.lookup(recommendations[i].product) for i in range(len(recommendations))]
    return recommended_recordings

def main(users_df, playcounts_df, recordings_df, users_q):
    global spark, sc
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    users_df.createOrReplaceTempView('user')
    playcounts_df.createOrReplaceTempView('playcount')
    recordings_map = recordings_df.rdd.map(lambda r: (r['recording_id'], (r['track_name'], r['recording_msid'])))
    with tempfile.TemporaryDirectory() as recommend_data:
        date = datetime.utcnow()
        hdfs_connection.init_hdfs(config.HDFS_HTTP_URI)
        models_dir = hdfs_connection.client.download(hdfs_path=os.path.join('/', 'data', 'listenbrainz', 'training-data-{}-{}-{}'.format(date.year,date.month,date.day), 'listenbrainz-recommendation-model'), local_path=recommend_data)
        print(models_dir)
        model = load_model(models_dir)
        for user in users_q.collect():
            print("user name: ", user.user_name)
            print(recommend_user(user.user_name, model, recordings_map))