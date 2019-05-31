import listenbrainz_spark
import os
import sys
import logging
import time
import uuid


from listenbrainz_spark import config
from pyspark.sql import Row, SparkSession
from datetime import datetime
from listenbrainz_spark.stats import run_query
from listenbrainz_spark.recommendations import utils
from time import sleep

def prepare_user_data(table):
    """ Prepare users dataframe to select distinct user names
        and assign each user a unique integer id.

        Args: 
            table: Registered dataframe to run SQL queries

        Returns: 
            users_df: users dataframe with columns as:
                user_id, user_name
    """
    users_df = run_query("""
            SELECT user_name
                  , row_number() over (ORDER BY "user_name") as user_id
             From (SELECT DISTINCT user_name FROM %s)
        """ % (table))
    return users_df

def prepare_listen_data(table):
    """ Prepare listens dataframe to select all the listens from
        the registered dataframe.

        Args: 
            table: Registered dataframe to run SQL queries

        Returns:
            listens_df: listens dataframe with columns as:
                listened_at, track_name, recording_msid, user_name   
    """
    listens_df = run_query("""
            SELECT listened_at
                 , artist_name
                 , artist_msid
                 , user_name
             From %s
        """ % (table))
    return listens_df

def prepare_artist_data(table):
    """ Prepare recordings dataframe to select distinct recordings/tracks
        listened to and assign each recording a unique integer id.

        Args: 
            table: Registered dataframe to run SQL queries

        Returns:
            recordings_df: recordings dataframe with columns as:
                track_name, recording_msid, artist_name, artist_msid, 
                release_name, release_msid, recording_id
    """
    artists_df = run_query("""
            SELECT artist_name
                 , artist_msid
                 , row_number() over (ORDER BY "artist_msid") AS artist_id
             From (SELECT DISTINCT artist_name, artist_msid FROM %s)
        """ % (table))
    return artists_df

def get_playcounts_data(listens_df, users_df, artists_df):
    """ Prepare playcounts dataframe by joining listens_df, users_df,
        recordings_df to select distinct tracks that a user has listened to
        for all the users along with listen count.

        Args:
            listens_df: Listens dataframe
            users_df: Users dataframe
            recordings_df: Recordings dataframe

        Returns:
            playcounts_df: playcounts dataframe with columns as:
                user_id, recording_id, count
    """
    listens_df.createOrReplaceTempView('listen')
    users_df.createOrReplaceTempView('user')
    artists_df.createOrReplaceTempView('artist')
    playcounts_df = run_query("""
        SELECT user_id,
               artist_id,
               count(artist_id) as count
          FROM listen
    INNER JOIN user
            ON listen.user_name = user.user_name
    INNER JOIN artist
            ON artist.artist_msid = listen.artist_msid
      GROUP BY user_id, artist_id
      ORDER BY user_id
    """)
    return playcounts_df

def main():
    ti = time.time()
    try:
        listenbrainz_spark.init_spark_session('Create_Dataframe')
    except Exception as err:
        raise SystemExit("Cannot initialize Spark Session: %s. Aborting..." % (str(err)))

    df = None
    for y in range(config.STARTING_YEAR, config.ENDING_YEAR + 1):
        for m in range(config.STARTING_MONTH, config.ENDING_MONTH + 1):
            try:
                month = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, y, m))
                df = df.union(month) if df else month
            except Exception as err:
                logging.error("Cannot read files from HDFS: %s / %s. Aborting." % (type(err).__name__, str(err)))
                continue
    if df is None:
        raise SystemExit("Parquet files containing listening history from {}-{} to {}-{} missing from HDFS".format(config.STARTING_YEAR, 
                    "%02d" % config.STARTING_MONTH, config.ENDING_YEAR, "%02d" % config.ENDING_MONTH))
    
    print("\nRegistering Dataframe...")
    table = 'df_to_train_{}'.format(datetime.strftime(datetime.utcnow(), '%Y_%m_%d'))
    df.createOrReplaceTempView(table)
    t = "%.2f" % (time.time() - ti)
    print("Dataframe registered in %ss" % (t))

    users_df = prepare_user_data(table)
    listens_df = prepare_listen_data(table)
    artists_df = prepare_artist_data(table)
    playcounts_df = get_playcounts_data(listens_df, users_df, artists_df)

    print("Preparing user data and saving to HDFS...")
    t0 = time.time()
    dest_path = os.path.join('/', 'data', 'listenbrainz', 'artist-similarity', 'dataframes', 'users_df.parquet')
    users_df.write.format('parquet').save(config.HDFS_CLUSTER_URI + dest_path, mode='overwrite')
    users_df_time = "%.2f" % ((time.time() - t0) / 60)

    print("Preparing artists dump and saving to HDFS...")
    t0 = time.time()
    dest_path = os.path.join('/', 'data', 'listenbrainz', 'artist-similarity', 'dataframes', 'artists_df.parquet')
    artists_df.write.format('parquet').save(config.HDFS_CLUSTER_URI + dest_path, mode='overwrite')
    artists_df_time =  "%.2f" % ((time.time() - t0) / 60)

    print("Preparing listen data dump and playcounts, saving playcounts to HDFS...")
    t0 = time.time()
    dest_path = os.path.join('/', 'data', 'listenbrainz', 'artist-similarity', 'dataframes', 'playcounts_df.parquet')
    playcounts_df.write.format('parquet').save(config.HDFS_CLUSTER_URI + dest_path, mode='overwrite')
    playcounts_df_time = "%.2f" % ((time.time() - t0) / 60)