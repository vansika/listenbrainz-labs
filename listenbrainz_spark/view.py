# This file aims to keep track of view names which are used in different modules
# and have been created from the same dataframe. Through this file we intend to avoid
# naming same view with different names. View names of dataframe stored in HDFS and used at
# multiple locations have been saved in here, if view names of intermediate dataframes are added
# at some point, please add a comment to indicate to which module or dataframe it belongs and a line
# to describe the usage of that view.
# Don't forget to append the date to recognize view creation day.
# Note: View here means a registered dataframe.
# Note: View name containing hyphen throw exception in Spark, use backticks(`) to escape view names with hyphen.
# NOte: We try to keep view names such so that it explains the type of data it contains.

from datetime import datetime

from listenbrainz_spark import config

date = datetime.strftime(datetime.utcnow(),'%Y-%m-%d')
# View created by registering dataframe containing listens which will be later used for training,
# validation and testing (after preprocessing).
LISTENS_TRAIN = '`train-listens-of-{}-days-{}`'.format(config.TRAIN_MODEL_WINDOW, date)
# View created by registering dataframe containing track information specific to a user.
LISTEN = '`listen-{}`'.format(date)
# View created by registering dataframe containing distinct user names.
USER = '`user-{}`'.format(date)
# View created by registering dataframe containing distinct recording msids.
RECORDING = '`recording-{}`'.format(date)

# Refer sql.create_dataframe_queries to know how the above mentioned views are created.
# These views have been created by querying LISTENS_TRAIN view.

# Since listens are stored month wise, we regitser a dataframe containing listens of months(s) of which listens of
# days on which we wish to generate recommendations is a subset.
LISTENS_MONTHS = '`user-listens-months(s)-{}`'.format(date)
# View created by registering dataframe containing listens of days on which we wish to generate the recommendations.
# This view is created by querying LISTENS_MONTHS.
LISTENS_DAYS = '`user-listens-{}-days-{}`'.format(config.RECOMMENDATION_GENERATION_WINDOW, date)
# View created by registering dataframe containing artist-artist relation i.e. similar artists.
ARTIST_RELATION = '`artist-relation-{}`'.format(date)

# View created by registering dataframe containing top artists listened to by a user.
TOP_ARTIST = '`top-artist-{}'.format(date)
# View created by registering dataframe containing artists similar to top artists listened to by a user.
SIMILAR_ARTIST = '`similar-artist-{}'.format(date)

# Note: The above mentioned views are specific to a user.
# Therefore, user name must be appended to these views before registering the dataframes.
# Closing backtick should be appended after the user name.
# Refer sql.candidate_set_queries to know how the above mentioned views are created.

# View created by registering dataframe containing user ids and recording ids of top artists listened to by a user for all users.
TOP_ARTIST_CANDIDATE = '`top-artist-candidate-{}`'.format(date)
# View created by registering dataframe containing user ids and recording ids of artists similar to top artists
# listened to by a user for all users.
SIMILAR_ARTIST_CANDIDATE = '`similar-artist-candidate-{}`'.format(date)

# Refer to sql.recommend_queries to know how the above mentioned views are created.
