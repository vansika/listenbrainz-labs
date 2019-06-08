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

def get_top_artist(table, user_name):
    top_artist_df = run_query("""
        SELECT user_name, artist_name, artist_msid, count(artist_msid) as count
            FROM %s
         GROUP BY user_name, artist_name, artist_msid
         HAVING user_name = '%s'
         ORDER BY count DESC
         LIMIT 50
    """ % (table, user_name))
    return top_artist_df

def get_candidate_recording_ids(table, candidate_artists):
    candidate_recording_ids = run_query("""
        SELECT recording_id
            FROM %s
         WHERE artist_name IN %s
    """ % (table, candidate_artists))
    return candidate_recording_ids

def get_user_id(table, user_name):
    result = run_query("""
        SELECT user_id
            FROM %s
         WHERE user_name = '%s'
    """% (table, user_name))
    return result.first()['user_id']

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
        artists_relation_df = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/similar_artists/artist_artist_relations.parquet'.format(config.HDFS_CLUSTER_URI))
    except AnalysisException as err:
        logging.error("Cannot read artist-artist relation from HDFS: %s \n %s. Aborting..." % (type(err).__name__,str(err)))
        sys.exit(-1)
    except Exception as err:
        logging.error("An error occured while fecthing artist-artist relation: %s \n %s. Aborting..." % (type(err).__name__, str(err)))
        sys.exit(-1)

    if artists_relation_df is None:
        raise SystemExit("Parquet file conatining artist-artist relation is missing from HDFS. Aborting...")

    try:
        path = os.path.join('/', 'data', 'listenbrainz', 'recommendation-engine', 'dataframes')
        recordings_df = listenbrainz_spark.sql_context.read.parquet(config.HDFS_CLUSTER_URI + path + '/recordings_df.parquet')
        users_df = listenbrainz_spark.sql_context.read.parquet(config.HDFS_CLUSTER_URI + path + '/users_df.parquet')
    except Exception as err:
        raise SystemExit("Cannot read dataframes from HDFS: %s. Aborting..." % (str(err)))

    print("\nRegistering Dataframe...")
    df_table = 'df_to_train_{}'.format(datetime.strftime(datetime.utcnow(), '%Y_%m_%d'))
    #artist_table = 'similar_artists_{}'.format(datetime.strftime(datetime.utcnow(), '%Y_%m_%d'))
    recordings_df_table = 'recordings_df{}'.format(datetime.strftime(datetime.utcnow(), '%Y_%m_%d'))
    users_df_table = 'users_df{}'.format(datetime.strftime(datetime.utcnow(), '%Y_%m_%d'))
    try:
        df.createOrReplaceTempView(df_table)
        #artists_relation_df.createOrReplaceTempView(artist_table)
        recordings_df.createOrReplaceTempView(recordings_df_table)
        users_df.createOrReplaceTempView(users_df_table)
    except Exception as err:
        logging.error("Cannot register dataframe: %s \n %s. Aborting..." % (type(err).__name__, str(err)), exc_info=True)
        sys.exit(-1)
    t = "%.2f" % (time.time() - ti)
    print("Files fectched from HDFS and dataframe registered in %ss" % (t))

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'recommendation-metadata.json')
    candidate_df = None


    schema = StructType([
        StructField("artist_name", StringType(), nullable=True),
        StructField("user_name", StringType(), nullable=True),
    ])

    with open(path) as f:
        recommendation_metadata = json.load(f)
        count = 0
        for user_name in recommendation_metadata['user_name']:
            top_artists = []
            candidate_artists = ()
            candidate_recordings_df = None
            try:
                top_artist_df = get_top_artist(df_table, user_name)
                if not top_artist_df.count():
                    continue
                for row in top_artist_df.collect():
                    top_artists.append(row.artist_name)
                    candidate_artists += (row.artist_name,)
                similar_artists = artists_relation_df[artists_relation_df.artist_name_0.isin(top_artists)].collect()
                for row in similar_artists:
                    candidate_artists += (row.artist_name_1,)
                candidate_recording_ids = get_candidate_recording_ids(recordings_df_table, candidate_artists)
                count += candidate_recording_ids.count()
                user_id = get_user_id(users_df_table, user_name)
                candidate_df_per_user = candidate_recording_ids.withColumn("user_id", lit(user_id))
                candidate_df = candidate_df.union(candidate_df_per_user) if candidate_df else candidate_df_per_user
                print("candidate_set generated for \'%s\'")
            except TypeError as err:
                logging.error("%s: Invalid user name. User \"%s\" does not exist." % (type(err).__name__,user_name))
            except Exception as err:
                logging.error("Candidate set for \"%s\" not generated.%s" % (user_name, str(err)))
        print(count)
        print(candidate_df.count())
    dest_path = os.path.join('/', 'data', 'listenbrainz', 'recommendation-engine', 'dataframes')
    candidate_df.write.format('parquet').save(config.HDFS_CLUSTER_URI + dest_path  + '/candidate_df.parquet', mode='overwrite')
