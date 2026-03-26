#!/usr/bin/env python3
"""
Custom Twitter streaming producer for robs-twitter.

Reads configuration entirely from environment variables so you never have
to edit this file:

  TWITTER_FOLLOW_ACCOUNTS  Comma-separated screen names to follow, e.g.
                           "NASA,BBCNews,elonmusk"

  TWITTER_TRACK_KEYWORDS   Comma-separated keywords to track (optional).
                           Combine with FOLLOW_ACCOUNTS or use alone.
                           e.g. "python,kafka"

  TWITTER_LANGUAGES        Comma-separated BCP-47 language codes to filter
                           (optional). Leave empty for all languages.
                           e.g. "en" or "en,es"

  BROKER_URL               Kafka broker, default "localhost:9092"
  TOPIC                    Kafka topic, default "twitterTopic"

  TWITTER_API_KEY / TWITTER_API_SECRET
  TWITTER_ACCESS_TOKEN_KEY / TWITTER_ACCESS_TOKEN_SECRET
"""

import os
import sys
import time

import tweepy
from confluent_kafka import Producer


# ── Config ────────────────────────────────────────────────────────────────────

def _csv(env_key, default=''):
    return [s.strip() for s in os.environ.get(env_key, default).split(',') if s.strip()]

BROKER_URL  = os.environ.get('BROKER_URL', 'localhost:9092')
TOPIC       = os.environ.get('TOPIC', 'twitterTopic')

TWITTER_API_KEY              = os.environ.get('TWITTER_API_KEY', '')
TWITTER_API_SECRET           = os.environ.get('TWITTER_API_SECRET', '')
TWITTER_ACCESS_TOKEN_KEY     = os.environ.get('TWITTER_ACCESS_TOKEN_KEY', '')
TWITTER_ACCESS_TOKEN_SECRET  = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET', '')

FOLLOW_ACCOUNTS = _csv('TWITTER_FOLLOW_ACCOUNTS')   # screen names
TRACK_KEYWORDS  = _csv('TWITTER_TRACK_KEYWORDS')     # keywords
LANGUAGES       = _csv('TWITTER_LANGUAGES')          # BCP-47, empty = all


# ── Kafka producer ────────────────────────────────────────────────────────────

class KafkaProducer:
    def __init__(self):
        self._producer = Producer({'bootstrap.servers': BROKER_URL})
        print(f'[kafka]   Connected to broker {BROKER_URL!r}')

    def send(self, data: str) -> None:
        self._producer.poll(0)
        self._producer.produce(TOPIC, data.encode('utf-8'), callback=self._report)

    @staticmethod
    def _report(err, msg) -> None:
        if err:
            print(f'[kafka]   Delivery failed: {err}')
        # Uncomment for verbose delivery confirmations:
        # else:
        #     print(f'[kafka]   Delivered → {msg.topic()} [{msg.partition()}]')


# ── Tweepy stream listener ────────────────────────────────────────────────────

class StreamListener(tweepy.StreamListener):
    def __init__(self, kafka: KafkaProducer):
        super().__init__()
        self._kafka = kafka
        self._count = 0

    def on_connect(self):
        print('[twitter] Stream connected — waiting for tweets…')

    def on_data(self, data: str) -> bool:
        try:
            self._kafka.send(data)
            self._count += 1
            if self._count % 10 == 0:
                print(f'[twitter] {self._count} tweets sent to Kafka so far')
        except Exception as exc:
            print(f'[producer] Send error: {exc}')
        return True

    def on_error(self, status_code: int) -> bool:
        print(f'[twitter] Stream error: HTTP {status_code}')
        if status_code == 420:
            # Rate-limited: back off and let Tweepy reconnect
            print('[twitter] Rate limited — backing off 60 s')
            time.sleep(60)
            return False   # disconnect; Tweepy will retry
        return True

    def on_timeout(self) -> bool:
        print('[twitter] Timeout — keeping stream alive')
        return True


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    # Validate credentials
    missing = [
        k for k in ('TWITTER_API_KEY', 'TWITTER_API_SECRET',
                    'TWITTER_ACCESS_TOKEN_KEY', 'TWITTER_ACCESS_TOKEN_SECRET')
        if not os.environ.get(k)
    ]
    if missing:
        print(f'[producer] ERROR: Missing env vars: {", ".join(missing)}')
        print('           Add them to your .env file and restart.')
        sys.exit(1)

    if not FOLLOW_ACCOUNTS and not TRACK_KEYWORDS:
        print('[producer] ERROR: Set at least one of TWITTER_FOLLOW_ACCOUNTS or TWITTER_TRACK_KEYWORDS')
        sys.exit(1)

    # Build Tweepy auth
    auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN_KEY, TWITTER_ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # Resolve screen names → numeric user IDs (required by the Streaming API)
    follow_ids = []
    for name in FOLLOW_ACCOUNTS:
        try:
            user = api.get_user(screen_name=name)
            follow_ids.append(str(user.id))
            print(f'[twitter] Will follow @{name} (id={user.id}, followers={user.followers_count:,})')
        except tweepy.TweepError as exc:
            print(f'[twitter] WARNING: Could not resolve @{name}: {exc}')

    if TRACK_KEYWORDS:
        print(f'[twitter] Will track keywords: {TRACK_KEYWORDS}')
    if LANGUAGES:
        print(f'[twitter] Language filter: {LANGUAGES}')
    else:
        print('[twitter] No language filter — receiving all languages')

    # Start streaming
    kafka    = KafkaProducer()
    listener = StreamListener(kafka)
    stream   = tweepy.Stream(auth=auth, listener=listener)

    filter_kwargs = {}
    if follow_ids:     filter_kwargs['follow']    = follow_ids
    if TRACK_KEYWORDS: filter_kwargs['track']     = TRACK_KEYWORDS
    if LANGUAGES:      filter_kwargs['languages'] = LANGUAGES

    print(f'[twitter] Starting stream…')
    # is_async=False keeps it blocking so Docker can manage restarts
    stream.filter(**filter_kwargs, is_async=False)


if __name__ == '__main__':
    main()
