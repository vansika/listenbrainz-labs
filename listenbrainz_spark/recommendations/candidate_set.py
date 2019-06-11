import listenbrainz_spark
import os
import sys
import logging
import time
import uuid
import json

from pyspark.sql.utils import *
from listenbrainz_spark import config
from datetime import datetime
from listenbrainz_spark.stats import run_query
from listenbrainz_spark.recommendations import utils
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from py4j.protocol import Py4JJavaError
from collections import defaultdict

def get_top_artist(user_name):
    top_artist_df = run_query("""
        SELECT user_name, artist_name, artist_msid, count(artist_msid) as count
            FROM df
         GROUP BY user_name, artist_name, artist_msid
         HAVING user_name = '%s'
         ORDER BY count DESC
         LIMIT 20
    """ % (user_name))
    return top_artist_df

def get_candidate_recording_ids(artists, user_id):
    df = run_query("""
        SELECT recording_id
            FROM recording
         WHERE artist_name IN %s
    """ % (artists,))
    candidate_recording_ids = df.withColumn("user_id", lit(user_id)) \
    .select("user_id", "recording_id")
    return candidate_recording_ids

def get_user_id(user_name):
    result = run_query("""
        SELECT user_id
            FROM user
         WHERE user_name = '%s'
    """% (user_name))
    return result.first()['user_id']

def get_all_artists():
    all_artists_df = run_query("""
        SELECT artist_name
            FROM recording
    """)
    return all_artists_df

def main():
    ti = time.time()
    try:
        listenbrainz_spark.init_spark_session('Candidate_set')
    except AttributeError as err:
        logging.error("Cannot initialize Spark Session: %s \n %s. Aborting..." % (type(err).__name__,str(err)), exc_info=True)
        sys.exit(-1)
    except Exception as err:
        logging.error("An error occurred: %s \n %s. Aborting..." % (type(err).__name__,str(err)), exc_info=True)
        sys.exit(-1)

    df = None
    for y in range(config.STARTING_YEAR, config.ENDING_YEAR + 1):
        for m in range(config.STARTING_MONTH, config.ENDING_MONTH + 1):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except AnalysisException as err:
                logging.error("Cannot read files from HDFS: %s \n %s" % (type(err).__name__,str(err)))
            except Exception as err:
                logging.error("An error occured while fecthing parquet: %s \n %s." % (type(err).__name__, str(err)))
                continue
    if df is None:
        raise SystemExit("Parquet files containing listening history from {}-{} to {}-{} missing from HDFS".format(config.STARTING_YEAR, 
                    "%02d" % config.STARTING_MONTH, config.ENDING_YEAR, "%02d" % config.ENDING_MONTH))

    artists_relation_df = None
    try:
        path = '{}/data/listenbrainz/similar_artists/artist_artist_relations.parquet'.format(config.HDFS_CLUSTER_URI)
        artists_relation_df = listenbrainz_spark.sql_context.read.parquet(path)
    except AnalysisException as err:
        logging.error("Cannot read artist-artist relation from HDFS: %s \n %s. Aborting..." % (type(err).__name__,str(err)))
        sys.exit(-1)
    except Exception as err:
        logging.error("An error occured while fecthing artist-artist relation: %s \n %s. Aborting..." % (type(err).__name__, str(err)))
        sys.exit(-1)

    if artists_relation_df is None:
        raise SystemExit("Parquet file conatining artist-artist relation is missing from HDFS. Aborting...")

    try:
        path = os.path.join('/', 'data', 'listenbrainz', 'recommendation-engine', 'dataframes' + '/')
        recordings_df = listenbrainz_spark.sql_context.read.parquet(config.HDFS_CLUSTER_URI + path + 'recordings_df.parquet')
        users_df = listenbrainz_spark.sql_context.read.parquet(config.HDFS_CLUSTER_URI + path + 'users_df.parquet')
    except Exception as err:
        raise SystemExit("Cannot read dataframes from HDFS: %s. Aborting..." % (str(err)))

    print("\nRegistering Dataframe...")
    try:
        df.createOrReplaceTempView('df')
        recordings_df.createOrReplaceTempView('recording')
        users_df.createOrReplaceTempView('user')
    except Exception as err:
        logging.error("Cannot register dataframe: %s \n %s. Aborting..." % (type(err).__name__, str(err)), exc_info=True)
        sys.exit(-1)
    t = "%.2f" % (time.time() - ti)
    print("Files fectched from HDFS and dataframe registered in %ss" % (t))

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'recommendation-metadata.json')
    user_data = defaultdict(dict)
    with open(path) as f:
        recommendation_metadata = json.load(f)
        for user_name in recommendation_metadata['user_name']:
            top_artists = ()
            similar_artists = ()
            new_artists = ()
            similar_artists_candidate_df = None
            top_artists_candidate_df = None
            new_artists_candidate_df = None
            try:
                ti = time.time()
                top_artist_df = get_top_artist(user_name)
                user_id = get_user_id(user_name)
                user_data[user_name] = defaultdict(dict)
                for row in top_artist_df.collect():
                    top_artists += (row.artist_name,)
                top_artists_recording_ids_df = get_candidate_recording_ids(top_artists, user_id) 
                top_artists_candidate_df = top_artists_candidate_df.union(top_artists_recording_ids_df) \
                                                if top_artists_candidate_df else top_artists_recording_ids_df
                t = "%.2f" % (time.time() - ti)
                user_data[user_name]['top_artists_time'] = t

                t0 = time.time()
                similar_artists_df = artists_relation_df[artists_relation_df.artist_name_0.isin(list(top_artists))]
                net_similar_artists_df = similar_artists_df.select("artist_name_1").subtract(top_artist_df.select("artist_name"))
                for row in net_similar_artists_df.collect():
                    similar_artists += (row.artist_name_1,)
                similar_artists_recording_ids_df = get_candidate_recording_ids(similar_artists, user_id)
                similar_artists_candidate_df = similar_artists_candidate_df.union(similar_artists_recording_ids_df) \
                                                    if similar_artists_candidate_df else similar_artists_recording_ids_df
                t = "%.2f" % (time.time() - t0)
                user_data[user_name]['similar_artists'] = defaultdict(dict)
                for artist in top_artists:
                    sim_artists = []
                    artists_df = artists_relation_df[artists_relation_df.artist_name_0.isin([artist])]
                    for row in artists_df.take(5):
                        sim_artists.append(row.artist_name_1)
                    user_data[user_name]['similar_artists'][artist]['names'] = sim_artists
                    user_data[user_name]['similar_artists'][artist]['count'] = "{:,}".format(artists_df.count())
                user_data[user_name]['similar_artists_count'] =  "{:,}".format(net_similar_artists_df.count())
                user_data[user_name]['similar_artists_time'] = t
                
                t0 = time.time()
                all_artists_df = get_all_artists()
                new_artists_df = all_artists_df.subtract(net_similar_artists_df)
                for row in new_artists_df.collect():
                    new_artists += (row.artist_name,)
                new_artists_recording_ids_df = get_candidate_recording_ids(new_artists, user_id)
                new_artists_candidate_df = new_artists_candidate_df.union(new_artists_recording_ids_df) \
                                                if new_artists_candidate_df else new_artists_recording_ids_df
                t = "%.2f" % (time.time() - t0)
                user_data[user_name]['new_artists_count'] = "{:,}".format(new_artists_df.count())
                user_data[user_name]['new_artists_time'] = t
                t = "%.2f" % (time.time() - ti)
                user_data[user_name]['total_time'] = t
                print("candidate_set generated for \'%s\'" % (user_name))
            except TypeError as err:
                logging.error("%s: Invalid user name. User \"%s\" does not exist." % (type(err).__name__,user_name))
            except Exception as err:
                logging.error("Candidate set for \"%s\" not generated.%s" % (user_name, str(err)))

    """
    dest_path = os.path.join(config.HDFS_CLUSTER_URI, 'data', 'listenbrainz', 'recommendation-engine', 'candidate-set' + '/')
    top_artists_candidate_df.write.format('parquet').save(dest_path  + 'top_artists.parquet', mode='overwrite')
    similar_artists_candidate_df.write.format('parquet').save(dest_path + 'similar_artists.parquet', mode=overwrite)
    new_artists_candidate_df.write.format('parquet').save(dest_path + 'new_artists.parquet', mode=overwrite)
    """

    date = datetime.utcnow().strftime("%Y-%m-%d")
    candidate_html = "Candidate-%s-%s.html" % (uuid.uuid4(), date)
    context = {
        'user_data' : user_data
    }
    utils.save_html(candidate_html, context, 'candidate.html')