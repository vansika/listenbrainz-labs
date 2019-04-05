import utils
import sys
import os
import tempfile
import time
import listenbrainz_spark

from pyspark.mllib.recommendation import MatrixFactorizationModel
from listenbrainz_spark import config
from datetime import datetime
from listenbrainz_spark.stats import run_query

def load_model(path):
    return MatrixFactorizationModel.load(listenbrainz_spark.context, path)

def get_user_id(user_name):
    result = run_query("""
        SELECT user_id
          FROM user
         WHERE user_name = '%s'
    """ % user_name)
    return result.first()['user_id']

def recommend_user(user_name, model, recordings_map):
    user_id = get_user_id(user_name)
    user_playcounts = run_query("""
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

def main(users_df, playcounts_df, recordings_df):
    users_df.createOrReplaceTempView('user')
    playcounts_df.createOrReplaceTempView('playcount')
    recordings_map = recordings_df.rdd.map(lambda r: (r['recording_id'], (r['track_name'], r['recording_msid'], r['artist_name'], r['artist_msid'])))
    date = datetime.utcnow()
    path = os.path.join('/', 'data', 'listenbrainz', 'listenbrainz-recommendation-model-{}-{}-{}'.format(date.year, date.month, date.day))
    model = load_model(config.HDFS_CLUSTER_URI + path)
    for user in users_df.collect():
        t0 = time.time()
        recommendations = recommend_user(user.user_name, model, recordings_map)
        for i in recommendations:
            print("Track name: \"{}\" , Recording MSID: \"{}\", Artist name: \"{}\", Artist MSID: \"{}\"".format(i[0][0], i[0][1], i[0][2], i[0][3]))
        print("Recommendations generated for {} in {}".format(user.user_name, time.time() - t0))