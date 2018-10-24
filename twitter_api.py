#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Creates JSON and TSV Files for the given Twitter user name.

Basically, it fetches all the tweets (not re-tweets) which were posted by
given user name, and creates JSON and TSV files accordingly. Finally, files
are posted to s3.

NOTE: "twitter_keys.json" has been ignored using .gitignore, so that you need
to create your own Twitter Secret Keys and Tokes. JSON file is something like
this at the end:

{"consumer_key": "abc",
 "consumer_secret": "def",
 "access_token": "tek",
 "access_token_secret": "klf"}

"""
import datetime
import json

import boto3
import pandas as pd
import pytz
import tweepy

TODAY_UTC = datetime.datetime.now(tz=pytz.utc)
TODAY_UTC = TODAY_UTC.astimezone(pytz.timezone('Europe/Helsinki'))
TODAY_UTC_STR = TODAY_UTC.strftime("%Y-%m-%d-%H%M%S_")

with open("twitter_keys.json") as credentials_file:
    CREDENTIALS = json.load(credentials_file)

AUTH = tweepy.OAuthHandler(CREDENTIALS['consumer_key'],
                           CREDENTIALS['consumer_secret'])
AUTH.set_access_token(CREDENTIALS['access_token'],
                      CREDENTIALS['access_token_secret'])
TWEEPY_API = tweepy.API(AUTH)

TWITTER_USER_NAME = "VRmatkalla"  # VR GROUP
# TWITTER_USER_NAME = "KleemolaAntti"  # CDO @ VR GROUP
# TWITTER_USER_NAME = "niinisto"  # President of Finland

JSON_OUTPUT_FILENAME = TODAY_UTC_STR + TWITTER_USER_NAME + ".json"
TSV_OUTPUT_FILENAME = TODAY_UTC_STR + TWITTER_USER_NAME + ".tsv"

BOTO3_SESSION = boto3.session.Session(region_name='eu-west-1')
BUCKET_NAME = "minerva-test-integration-in-snowpipe-demo"
JSON_S3_KEY = "data/raw/{}".format(JSON_OUTPUT_FILENAME)
TSV_S3_KEY = "data/tsv/{}".format(TSV_OUTPUT_FILENAME)

ATTRIBUTES = ['text',
              'created_at',
              'geo',
              'lang',
              'coordinates',
              'user.favourites_count',
              'user.statuses_count',
              'user.description',
              'user.location',
              'user.id',
              'user.created_at',
              'user.verified',
              'user.following',
              'user.url',
              'user.listed_count',
              'user.followers_count',
              'user.default_profile_image',
              'user.utc_offset',
              'user.friends_count',
              'user.default_profile',
              'user.name',
              'user.lang',
              'user.screen_name',
              'user.geo_enabled',
              'user.profile_background_color',
              'user.profile_image_url',
              'user.time_zone',
              'id',
              'favorite_count',
              'retweeted',
              'source',
              'favorited',
              'retweet_count']


def create_json_file(all_tweets, json_file_name):
    """ Creates JSON File for all the tweets.
    :param all_tweets: All tweets (list of dictionaries) E.g. [{..}, {..}]
    :type all_tweets: (list)
    :param json_file_name: Output JSON file name.
    :type json_file_name: (str)
    :return: (None)
    """
    with open(json_file_name, 'w') as outfile:
        json.dump(all_tweets, outfile)


def create_tsv_file(all_tweets, tsv_file_name):
    """ Creates TSV File for all the tweets.
    :param all_tweets: All tweets (list of dictionaries) E.g. [{..}, {..}]
    :type all_tweets: (list)
    :param tsv_file_name: Output TSV file name.
    :type tsv_file_name: (str)
    :return: (None)
    """
    data_frame = pd.DataFrame(all_tweets)
    desired_columns = ['twitter_user_name', 'text', 'lang', 'created_at']
    data_frame = data_frame[desired_columns]
    data_frame = data_frame.rename(columns={'text': 'tweet',
                                            'lang': 'tweet_language',
                                            'created_at': 'tweet_timestamp'})
    data_frame['tweet_timestamp'] = \
        pd.to_datetime(data_frame['tweet_timestamp'],
                       format="%Y-%m-%d %H:%M:%S")
    # Replace new line with space
    data_frame.tweet = data_frame.tweet.replace('\n', ' ', regex=True)

    data_frame.to_csv(tsv_file_name, sep='\t', index=False, encoding='utf-8')


def upload_to_s3(local_file_path, bucket, key):
    """ Upload local file to S3.
    :param local_file_path: (str) Full path to uploaded file
    :param bucket: (str) S3 Bucket
    :param key: (str) S3 key
    :return: None
    """
    s3 = BOTO3_SESSION.client('s3')
    s3.upload_file(local_file_path, bucket, key)


def main():
    """ Create JSON and TSV files. Finally, files are posted to S3. """
    all_tweets = []
    for status in tweepy.Cursor(TWEEPY_API.user_timeline,
                                id=TWITTER_USER_NAME).items():
        # If the tweet is not a re-tweet
        if 'RT @' not in status.text:
            tweet = {}
            for column in ATTRIBUTES:
                try:
                    tmp_col_value = getattr(status, column)

                    # datetime to isoformat for JSON data
                    if isinstance(tmp_col_value, datetime.datetime):
                        tmp_col_value = tmp_col_value.isoformat()
                except Exception:
                    tmp_col_value = ""

                tweet.update({column: tmp_col_value})

            tweet.update({'twitter_user_name': TWITTER_USER_NAME})
            all_tweets.append(tweet)

    print("Twitter User Name - {} - {} Tweets".format(TWITTER_USER_NAME,
                                                      len(all_tweets)))

    create_json_file(all_tweets, JSON_OUTPUT_FILENAME)
    upload_to_s3(JSON_OUTPUT_FILENAME, BUCKET_NAME, JSON_S3_KEY)

    create_tsv_file(all_tweets, TSV_OUTPUT_FILENAME)
    upload_to_s3(TSV_OUTPUT_FILENAME, BUCKET_NAME, TSV_S3_KEY)


if __name__ == '__main__':
    main()
