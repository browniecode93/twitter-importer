from datetime import timedelta
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
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


def get_last_tweet_id(conn_id, username):
    hook = MongoHook(conn_id=conn_id)

    marker = hook.find(
        mongo_collection='marker', query={'importer_key': f'last_tweet_id_{username}'}, find_one=True,
        mongo_db='test',
    )
    if marker:
        return marker['last_id']
    return


def first_time_getting_tweets(conn_id, api, username):
    all_tweets = []

    new_tweets = api.user_timeline(screen_name=username, count=300)

    all_tweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    if len(all_tweets) > 0:
        last_id = all_tweets[-1].id - 1
    else:
        print(f"The username you've provided ({username}) has not any tweet so far.")
        return

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        new_tweets = api.user_timeline(screen_name=username, count=300, max_id=last_id)

        print(f'getting tweets before {last_id}')

        # save most recent tweets
        all_tweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        last_id = all_tweets[-1].id - 1

        print(f'...{len(all_tweets)} tweets downloaded so far')

    latest_id = all_tweets[0].id + 1

    if latest_id:
        hook = MongoHook(conn_id=conn_id)
        hook.insert_one(
            mongo_collection='marker',
            doc={'importer_key': f'last_tweet_id_{username}', 'last_id': latest_id},
            mongo_db='test',
        )
    out_tweets = [
        {
            'tw_id': tweet.id_str, 'tw_created_at': tweet.created_at, 'tw_text': tweet.text.encode('utf-8'),
            'tw_user': tweet.author._json['screen_name'],
        } for tweet in all_tweets
    ]
    return out_tweets


def get_tweets(last_id, conn_id, api, username):
    all_tweets = []

    # make initial request for most recent tweets greater than an id
    new_tweets = api.user_timeline(screen_name=username, count=300, since_id=last_id)

    print(f'getting tweets after {last_id}')

    all_tweets.extend(new_tweets)

    print(f'...{len(all_tweets)} tweets downloaded so far')

    # save the id of the oldest tweet less one
    if all_tweets and len(all_tweets) > 0:
        last_id = all_tweets[0].id + 1
    else:
        print(f"The username you've provided ({username}) has not any new tweet.")
        return

    hook = MongoHook(conn_id=conn_id)
    hook.update_one(
        mongo_collection='marker',
        filter_doc={'importer_key': f'last_tweet_id_{username}'},
        update_doc={'$set': {'last_id': last_id}},
        mongo_db='test',
    )
    out_tweets = [
        {
            'tw_id': tweet.id_str, 'tw_created_at': tweet.created_at, 'tw_text': tweet.text.encode('utf-8'),
            'tw_user': tweet.author._json['screen_name'],
        } for tweet in all_tweets
    ]
    return out_tweets


def insert_to_mongo(**kwargs):
    conn_id = kwargs['conn_id']
    hook = MongoHook(conn_id)
    api = get_api_object()
    usernames = ['ApacheAirflow', 'Airbnb']
    for username in usernames:
        print(f'Import tweets for {username}')
        last_id = get_last_tweet_id(conn_id, username)
        if not last_id:
            out_tweets = first_time_getting_tweets(conn_id, api, username)
        else:
            out_tweets = get_tweets(last_id, conn_id, api, username)
        print(f'All tweets are {out_tweets}')
        if out_tweets and len(out_tweets) > 0:
            hook.insert_many(
                mongo_collection='twitter',
                docs=out_tweets,
                mongo_db='test',
            )

        new_importer_key = hook.find(
            mongo_collection='marker', query={'importer_key': f'last_tweet_id_{username}'},
            find_one=True, mongo_db='test',
        )
        print(f'The new importer key for {username} is {new_importer_key}')


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 10,
    'retry_delay': timedelta(minutes=30),
}

dag_params = {
    'dag_id': 'get_user_timeline',
    'default_args': default_args,
    'schedule_interval': '@daily',
    'catchup': False,
    'max_active_runs': 1,
}

with DAG(**dag_params) as dag:
    insert_row = PythonOperator(
        task_id='insert_to_mongo',
        python_callable=insert_to_mongo,
        op_kwargs={
            'conn_id': 'mongo_default',
        },
        dag=dag,
    )
