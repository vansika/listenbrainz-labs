import sys
import listenbrainz_spark
import time
import json

from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
from listenbrainz_spark.stats_writer.stats_writer import StatsWriter
from listenbrainz_spark import config
from listenbrainz_spark.stats import run_query


def get_artists(table):
    """
    Args:
        table: name of the temporary table

    Returns:
        artist (dict): listen_count and statistics of
               artists listened in the previous month
    """
    t0 = time.time()
    query = run_query("""
            SELECT user_name
                 , artist_name
                 , artist_msid
                 , artist_mbids
                 , count(artist_name) as cnt
              FROM %s
          GROUP BY user_name, artist_name, artist_msid, artist_mbids
          ORDER BY cnt DESC
        """ % (table))
    artist = defaultdict(list)
    query.show()
    t = time.time()
    rows = query.collect()
    print("time taken by collect call: %.2f" % (time.time() - t))
    for row in rows:
        artist[row.user_name].append({
            'artist_name': row.artist_name,
            'artist_msid': row.artist_msid,
            'artist_mbids': row.artist_mbids,
            'listen_count': row.cnt,
        })
    query_t0 = time.time()
    print("Query to calculate artist stats proccessed in %.2f s" % (query_t0 - t0))
    return artist

def main(app_name):
    """
    Args:
        app_name: Application name to uniquely identify the submitted
                application amongst other applications

    Returns:
        stats (list): list of dicts where each dict is a message/packet
                    for RabbitMQ containing user stats
    """
    t0 = time.time()
    listenbrainz_spark.init_spark_session(app_name)
    t = datetime.utcnow().replace(day=1)
    date = t + relativedelta(months=-1)
    try:
        df = listenbrainz_spark.sql_context.read.parquet('{}/data/listenbrainz/{}/{}.parquet'.format(config.HDFS_CLUSTER_URI, date.year, date.month))
        print("Loading dataframe...")
    except:
        print ("No Listens for last month")
        sys.exit(-1)
    df.printSchema()
    print(df.columns)
    print(df.count())
    table = 'listens_{}'.format(datetime.strftime(date, '%Y_%m'))
    print(table)
    df.registerTempTable(table)
    print("Running Query...")
    query_t0 = time.time()
    print("DataFrame loaded in %.2f s" % (query_t0 - t0))
    obj = StatsWriter()
    yearmonth = datetime.strftime(date, '%Y-%m')
    artists = get_artists(table)
    for key, value in artists.items():
        user_data = {}
        user_data[key] = {}
        user_data[key]['artists'] = {}
        user_data[key]['yearmonth'] = yearmonth
        user_data[key]['artists']['artist_stats'] = value
        user_data[key]['artists']['artist_count'] = len(value)
        print (user_data)
    