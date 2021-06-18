from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.python_operator import PythonOperator
import tweepy
import json

auth = tweepy.OAuthHandler('dcf', 'dcf')
auth.set_access_token('abc', 'abc')


def get_last_tweet_id(conn_id, **kwargs):
    hook = MongoHook(conn_id=conn_id)

    marker = hook.find(mongo_collection='marker', query={"importer_key": "last_tweet_id"}, find_one=True, mongo_db='test')
    if marker:
        return marker['last_id']
    return

def first_time_getting_tweets(conn_id, **kwargs):
    
    api = tweepy.API(auth)

    alltweets = []

    new_tweets = api.user_timeline(screen_name='mstootfarangi', count=300)

    alltweets.extend(new_tweets)

   #save the id of the oldest tweet less one
    if len(alltweets) > 0:
        last_id = alltweets[-1].id - 1
    else:
        print("The username you've provided has not any tweet so far.")
        return

    #keep grabbing tweets until there are no tweets left to grab
    while (len(new_tweets) > 0) :

        new_tweets = api.user_timeline(screen_name = 'mstootfarangi', count=300, max_id=last_id)

        print(f"getting tweets before {last_id}")

        #save most recent tweets
        alltweets.extend(new_tweets)

        #update the id of the oldest tweet less one
        last_id = alltweets[-1].id - 1

        print(f"...{len(alltweets)} tweets downloaded so far")

    latest_id = alltweets[0].id + 1

    if latest_id:
        hook = MongoHook(conn_id=conn_id)
        hook.insert_one(                                                                            
            mongo_collection='marker',                                                                       
            doc={"importer_key": "last_tweet_id", "last_id": latest_id},                                                       
            mongo_db='test'                                                                              
        )
    outtweets = [{'tw_id':tweet.id_str, 'tw_created_at': tweet.created_at, 'tw_text': tweet.text, 'tw_user': tweet.author._json['screen_name']} for tweet in alltweets]
    return outtweets
    

def get_tweets(last_id, conn_id, **kwargs):

    api = tweepy.API(auth)

    alltweets = []
    
    #make initial request for most recent tweets greater than an id
    new_tweets = api.user_timeline(screen_name='mstootfarangi', count=300, since_id=last_id)

    print(f"getting tweets after {last_id} hereeee2")
    
    alltweets.extend(new_tweets)

    print(f"...{len(alltweets)} tweets downloaded so far")

   #save the id of the oldest tweet less one
    if alltweets and len(alltweets) > 0:
        last_id = alltweets[0].id + 1
    else:
        print("The username you've provided has not new tweet.")
        return

    hook = MongoHook(conn_id=conn_id)
    hook.update_one(
        mongo_collection='marker',                                                                       
        filter_doc={"importer_key": "last_tweet_id"},
        update_doc={"$set": {'last_id': last_id}},
        mongo_db='test'                                                                              
    )
    outtweets = [{'tw_id': tweet.id_str, 'tw_created_at': tweet.created_at, 'tw_text': tweet.text, 'tw_user': tweet.author._json['screen_name']} for tweet in alltweets]
    return outtweets


def insert_to_mongo(**kwargs):
    conn_id = kwargs['conn_id']
    hook = MongoHook(conn_id)
    
    last_id = get_last_tweet_id(conn_id)
    outtweets = []
    if not last_id:
        outtweets = first_time_getting_tweets(conn_id)
    else:
        outtweets = get_tweets(last_id, conn_id)
    print(f'All tweets are {outtweets}')
    if(outtweets and len(outtweets)>0):
        a = hook.insert_many(
            mongo_collection='twitter',
            docs=outtweets,
            mongo_db='test'
        )

    finded = hook.find(mongo_collection='marker', query={"importer_key": "last_tweet_id"}, find_one=True, mongo_db='test')
    print("Hi")
    print(finded)

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
    insert_row = PythonOperator(
        task_id='insert_to_mongo',
        python_callable=insert_to_mongo,
        op_kwargs={
            'conn_id': 'mongo_default'
        },
        dag=dag,
    )

