from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.python_operator import PythonOperator

import tweepy

def get_api_object():
    consumer_key = 'consumer_key'
    consumer_secret = 'consumer_secret'
    access_token = 'access_token'
    access_token_secret = 'access_token_secret'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api


def get_latest_tweet_time(conn_id, hashtag):
    hook = MongoHook(conn_id=conn_id)

    marker = hook.find(mongo_collection='marker', query={"importer_key": f"latest_tweet_time_{hashtag}"}, find_one=True, mongo_db='test')
    if marker:
        return marker['last_time']
    return


def getting_hashtags(conn_id, api, hashtags):

    alltweets = []
    for hashtag in hashtags:
        latest_created_at = get_latest_tweet_time(conn_id, hashtag)

        if latest_created_at:
            new_tweets = tweepy.Cursor(api.search, q=hashtag).items()
        else:
            new_tweets = tweepy.Cursor(api.search, q=hashtag, since=latest_created_at).items()

        alltweets.extend(new_tweets)

        #save the latest created_at
        if len(alltweets) > 0:
            latest_tweet_time = alltweets[0].created_at
        else:
            print(f"The hashtag you've provided ({hashtag}) has not any tweet so far.")
            return

        print(f"...{len(alltweets)} tweets downloaded so far")

        if latest_tweet_time:
            hook = MongoHook(conn_id=conn_id)
            hook.update_one(
                mongo_collection='marker',
                filter_doc={"importer_key": f"latest_tweet_time_{hashtag}"},
                update_doc={"$set": {'last_time': latest_tweet_time}},
                mongo_db='test'
            )
        outtweets = [{'tw_hashtags':tweet.entities['hashtags'], 'tw_id':tweet.id_str, 'tw_created_at': tweet.created_at, 'tw_text': tweet.text.encode('utf-8'), 'tw_user': tweet.user['screen_name'], 'tw_location': tweet.user.location} for tweet in alltweets]

    return outtweets


def insert_to_mongo(**kwargs):
    conn_id = kwargs['conn_id']
    outtweets = kwargs['outtweets']
    hook = MongoHook(conn_id)

    if outtweets and len(outtweets)>0:
        hook.insert_many(
        mongo_collection='twitter',
        docs=outtweets,
        mongo_db='test'
    )

"""Default arguments used on DAG parameters."""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 480,
    'retry_delay': timedelta(minutes=30),
}

dag_params = {
    'dag_id': 'mongo',
    'default_args': default_args,
    'schedule_interval': '@daily',
    'catchup': False,
    'max_active_runs': 1,
}
"""Default parameters used on DAG instance."""
with DAG(**dag_params) as dag:

    get_api = PythonOperator(
        task_id='get_api_object',
        python_callable=get_api_object,
        op_kwargs={
            'conn_id': 'mongo_default'
        },
        dag=dag,
    )

    get_hashtags = PythonOperator(
        task_id='get_hashtags',
        python_callable=getting_hashtags,
        op_kwargs={
            'conn_id': 'mongo_default'
        },
        dag=dag,
    )

    insert_rows = PythonOperator(
        task_id='insert_to_mongo',
        python_callable=insert_to_mongo,
        op_kwargs={
            'conn_id': 'mongo_default'
        },
        dag=dag,
    )

    get_api >> get_hashtags >> insert_rows

