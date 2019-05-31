import os
import sys
import json
import listenbrainz_spark
import numpy as np
import logging
import uuid

from numpy import linalg as LA
from listenbrainz_spark.stats import run_query
from listenbrainz_spark import config
from time import time
from datetime import datetime
from listenbrainz_spark.artist_similarity import utils

def get_user_id(user_name):
    result = run_query("""
        SELECT user_id
          FROM user
         WHERE user_name = '%s'
    """ % user_name)
    return result.first()['user_id']

def get_top_artists(user_id):
    artists_df = run_query("""
            SELECT artist_id
              FROM playcount
             WHERE user_id = '%s'
            ORDER BY count DESC
            LIMIT 10
        """ % user_id)
    return artists_df

def cosineSimilarity(vec1, vec2):
    return vec1.dot(vec2) / (LA.norm(vec1) * LA.norm(vec2))

def main():
    try:
        listenbrainz_spark.init_spark_session('Artist_Dataframe')
    except Exception as err:
        raise SystemExit("Cannot initialize Spark Session: %s. Aborting..." % (str(err)))
    
    print("Fetching product Features...")
    productFeatures = None
    try:
        productFeatures = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/artist-similarity/best_model/listenbrainz-recommendation-model/data/product'.format(config.HDFS_CLUSTER_URI))
        artists_df = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/artist-similarity/dataframes/artists_df.parquet'.format(config.HDFS_CLUSTER_URI))
        playcounts_df = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/artist-similarity/dataframes/playcounts_df.parquet'.format(config.HDFS_CLUSTER_URI))
        users_df = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/artist-similarity/dataframes/users_df.parquet'.format(config.HDFS_CLUSTER_URI))
    except Exception as err:
        raise SystemExit("Cannot read dataframe from HDFS: %s. Aborting..." % (str(err)))
    if productFeatures is None:
        raise SystemExit("ProductFeatures is empty. Aborting...")

    playcounts_df.createOrReplaceTempView('playcount')
    users_df.createOrReplaceTempView('user')
    print("recordings_df count: ", artists_df.count())
    print("product features count: ", productFeatures.count())
    path = os.path.join('/', 'rec', 'listenbrainz_spark', 'recommendations', 'recommendation-metadata.json')
    with open(path) as f:
        recommendation_metadata = json.load(f)
        user_info = {}
        for user_name in recommendation_metadata['user_name']:
            try:
                candidate_artist_ids = []
                user_id = get_user_id(user_name)
                top_artists = get_top_artists(user_id)
                for row in top_artists.collect():
                    itemFactor = np.asarray(productFeatures.rdd.lookup(row.artist_id))[0]
                    artist_similarity = productFeatures.rdd.map(lambda products: (products[0],
                        cosineSimilarity(np.asarray(products[1]), itemFactor)) ).takeOrdered(100, key=lambda x: -x[1]) 
                    for similar_artist in artist_similarity:
                        candidate_artist_ids.append(similar_artist[0])
                candidate_df = artists_df[artists_df.artist_id.isin(candidate_artist_ids)]
                candidate_artists = []
                for row in candidate_df.collect():
                    rec = (row.artist_name, row.artist_msid)
                    candidate_artists.append(rec)
                user_info[user_name] = candidate_artists
            except TypeError as err:
                logging.error("%s: Invalid user name. User \"%s\" does not exist." % (type(err).__name__,user_name))
            except Exception as err:
                logging.error("A problem occured while fetching similar artists for \"%s\" : %s" % (user_name, str(err)))
    date = datetime.utcnow().strftime("%Y-%m-%d")
    artist_html = 'Similar_Artists-%s-%s.html' % (uuid.uuid4(), date)

    context = {
        'user_info' : user_info
    }
    utils.save_html(artist_html, context, 'artist_similarity.html')