import os
import sys
import click
import logging

import listenbrainz_spark
from listenbrainz_spark import utils

app = utils.create_app(debug=True)

@click.group()
def cli():
    pass

@cli.command(name='dataframe')
def dataframes():
    """ Invoke script responsible for pre-processing data.
    """
    from listenbrainz_spark.recommendations import create_dataframes
    with app.app_context():
        create_dataframes.main()

@cli.command(name='model')
def model():
    """ Invoke script responsible for training data.
    """
    from listenbrainz_spark.recommendations import train_models
    with app.app_context():
        train_models.main()

@cli.command(name='candidate')
def candidate():
    """ Invoke script responsible for generating candidate sets.
    """
    from listenbrainz_spark.recommendations import candidate_sets
    with app.app_context():
        candidate_sets.main()

@cli.command(name='recommend')
def recommend():
    """ Invoke script responsible for generating recommendations.
    """
    from listenbrainz_spark.recommendations import recommend
    with app.app_context():
        recommend.main()

@cli.command(name='user')
def user():
    """ Invoke script responsible for calculating user statistics.
    """
    from listenbrainz_spark.stats import user
    with app.app_context:
        user.main()

@cli.resultcallback()
def remove_zip(result, **kwargs):
    """ Remove zip created by spark-submit.
    """
    os.remove(os.path.join('/', 'rec', 'listenbrainz_spark.zip'))

if __name__ == '__main__':
    # The root logger always defaults to WARNING level
    # The level is changed from WARNING to INFO
    logging.getLogger().setLevel(logging.INFO)
    cli()



