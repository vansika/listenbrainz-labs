from listenbrainz_spark.stats import run_query

def get_user_id(user_name, user_view):
    """ Get user id using user name.

        Args:
            user_name: Name of the user.

        Returns:
            user_id: User id of the user.
    """
    result = run_query("""
        SELECT user_id
          FROM {}
         WHERE user_name = '{}'
    """.format(user_view, user_name))
    return result.first()['user_id']
