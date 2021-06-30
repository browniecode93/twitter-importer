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

    marker = hook.find(mongo_collection='marker', query={"importer_key": f"latest_tweet_time_{hashtag}"}, find_one=True,
                       mongo_db='test')
    if marker:
        return marker['last_time']
    return


def getting_hashtags(conn_id, hashtags, **kwargs):
    api = get_api_object()
    alltweets = []
    last_hashtag_index = 0
    for hashtag in hashtags:
        latest_created_at = get_latest_tweet_time(conn_id, hashtag)
        print(f'The last tweet time for {hashtag} is {latest_created_at}')
        if not latest_created_at:
            new_tweets = tweepy.Cursor(api.search, q=hashtag).items(750)
        else:
            new_tweets = tweepy.Cursor(api.search, q=hashtag, since=latest_created_at).items(750)

        alltweets.extend(new_tweets)
        len_all_tweets = len(alltweets)
        if len_all_tweets > 0:
            print(f'Number of new tweets is {len_all_tweets - last_hashtag_index + 1}')
            latest_tweet_time = alltweets[last_hashtag_index].created_at
            # Saving the index of the earliest tweet of a hashtag
            last_hashtag_index = len_all_tweets + 1
        else:
            print(f"The hashtag you've provided ({hashtag}) has not any new tweet so far.")
            continue

        print(f"...{len_all_tweets} tweets downloaded so far")

        if latest_tweet_time:
            hook = MongoHook(conn_id=conn_id)
            hook.update_one(
                mongo_collection='marker',
                filter_doc={"importer_key": f"latest_tweet_time_{hashtag}"},
                update_doc={"$set": {'last_time': latest_tweet_time.strftime('%Y-%m-%d')}},
                upsert=True,
                mongo_db='test'
            )
    outtweets = [{'tw_hashtags': tweet.entities['hashtags'], 'tw_id': tweet.id_str, 'tw_created_at': tweet.created_at,
                  'tw_text': tweet.text, 'tw_user': tweet.author._json['screen_name'],
                  'tw_location': tweet.author._json['location']} for tweet in alltweets]

    kwargs['task_instance'].xcom_push(key='all_tweets', value=outtweets)


def insert_to_mongo(**kwargs):
    conn_id = kwargs['conn_id']
    outtweets = kwargs['task_instance'].xcom_pull(task_ids='get_hashtags', key='all_tweets')
    hook = MongoHook(conn_id)
    filter_docs = [{'tw_id': doc['tw_id']} for doc in outtweets]
    if outtweets and len(outtweets) > 0:
        a = hook.replace_many(
            mongo_collection='twitter',
            docs=outtweets,
            filter_docs=filter_docs,
            mongo_db='test',
            upsert=True
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
    'dag_id': 'mongo_hashtags',
    'default_args': default_args,
    'schedule_interval': '@daily',
    'catchup': False,
    'max_active_runs': 1,
}
"""Default parameters used on DAG instance."""
with DAG(**dag_params) as dag:
    get_hashtags = PythonOperator(
        task_id='get_hashtags',
        python_callable=getting_hashtags,
        provide_context=True,
        op_kwargs={
            'conn_id': 'mongo_default',
            'hashtags': ['#unitedAIRLINES', '#Flight', '#beach']
        },
        dag=dag,
    )

    insert_rows = PythonOperator(
        task_id='insert_to_mongo',
        python_callable=insert_to_mongo,
        provide_context=True,
        op_kwargs={
            'conn_id': 'mongo_default',
        },
        dag=dag,
    )

    get_hashtags >> insert_rows

