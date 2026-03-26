#!/usr/bin/env python3
"""
Custom Twitter streaming producer for robs-twitter.

Configuration priority (highest to lowest):
  1. /config/stream-config.json  — written by the frontend UI
  2. Environment variables        — set in .env / docker-compose

The producer watches the config file every CONFIG_POLL_INTERVAL seconds.
When a change is detected it disconnects the current stream and reconnects
with the updated settings — no container restart needed.

Environment variables
─────────────────────
TWITTER_API_KEY / TWITTER_API_SECRET
TWITTER_ACCESS_TOKEN_KEY / TWITTER_ACCESS_TOKEN_SECRET

TWITTER_FOLLOW_ACCOUNTS   Comma-separated screen names  e.g. "NASA,BBCNews"
TWITTER_TRACK_KEYWORDS    Comma-separated keywords       e.g. "python,kafka"
TWITTER_LANGUAGES         Comma-separated BCP-47 codes  e.g. "en"  (empty = all)

BROKER_URL                Kafka broker  (default: localhost:9092)
TOPIC                     Kafka topic   (default: twitterTopic)
CONFIG_PATH               Path to shared config JSON  (default: /config/stream-config.json)
CONFIG_POLL_INTERVAL      Seconds between config checks  (default: 15)
"""

import json
import os
import sys
import threading
import time

import tweepy
from confluent_kafka import Producer


# ── Environment defaults ──────────────────────────────────────────────────────

def _csv(key, default=''):
    return [s.strip() for s in os.environ.get(key, default).split(',') if s.strip()]

BROKER_URL  = os.environ.get('BROKER_URL', 'localhost:9092')
TOPIC       = os.environ.get('TOPIC', 'twitterTopic')

TWITTER_API_KEY             = os.environ.get('TWITTER_API_KEY', '')
TWITTER_API_SECRET          = os.environ.get('TWITTER_API_SECRET', '')
TWITTER_ACCESS_TOKEN_KEY    = os.environ.get('TWITTER_ACCESS_TOKEN_KEY', '')
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET', '')

CONFIG_PATH          = os.environ.get('CONFIG_PATH', '/config/stream-config.json')
CONFIG_POLL_INTERVAL = int(os.environ.get('CONFIG_POLL_INTERVAL', '15'))

# Env-var fallbacks (used if the config file doesn't exist yet)
ENV_FOLLOW_ACCOUNTS = _csv('TWITTER_FOLLOW_ACCOUNTS')
ENV_TRACK_KEYWORDS  = _csv('TWITTER_TRACK_KEYWORDS')
ENV_LANGUAGES       = _csv('TWITTER_LANGUAGES')


# ── Config file helpers ───────────────────────────────────────────────────────

def load_config():
    """Return (follow_accounts, track_keywords, languages) from file or env."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            data = json.load(f)
        return (
            [s.strip() for s in data.get('followAccounts', []) if s.strip()],
            [s.strip() for s in data.get('trackKeywords',  []) if s.strip()],
            [s.strip() for s in data.get('languages',      []) if s.strip()],
        )
    except (FileNotFoundError, json.JSONDecodeError):
        return ENV_FOLLOW_ACCOUNTS, ENV_TRACK_KEYWORDS, ENV_LANGUAGES


def config_mtime():
    try:
        return os.path.getmtime(CONFIG_PATH)
    except FileNotFoundError:
        return 0.0


# ── Kafka producer ────────────────────────────────────────────────────────────

class KafkaProducer:
    def __init__(self):
        self._p = Producer({'bootstrap.servers': BROKER_URL})
        print(f'[kafka]   Connected to {BROKER_URL!r}')

    def send(self, data: str) -> None:
        self._p.poll(0)
        self._p.produce(TOPIC, data.encode('utf-8'), callback=self._report)

    @staticmethod
    def _report(err, msg) -> None:
        if err:
            print(f'[kafka]   Delivery failed: {err}')


# ── Tweepy stream listener ────────────────────────────────────────────────────

class StreamListener(tweepy.StreamListener):
    def __init__(self, kafka: KafkaProducer, stop_event: threading.Event):
        super().__init__()
        self._kafka = kafka
        self._stop  = stop_event
        self._count = 0

    def on_connect(self):
        print('[twitter] Stream connected — waiting for tweets…')

    def on_data(self, data: str) -> bool:
        if self._stop.is_set():
            return False  # disconnect
        try:
            self._kafka.send(data)
            self._count += 1
            if self._count % 10 == 0:
                print(f'[twitter] {self._count} tweets forwarded to Kafka')
        except Exception as exc:
            print(f'[producer] Send error: {exc}')
        return True

    def on_error(self, status_code: int) -> bool:
        print(f'[twitter] Stream error: HTTP {status_code}')
        if status_code == 420:
            print('[twitter] Rate limited — backing off 60 s')
            time.sleep(60)
            return False
        return True

    def on_timeout(self) -> bool:
        return True


# ── Account resolution ────────────────────────────────────────────────────────

def resolve_ids(api, screen_names):
    """Resolve a list of screen names to numeric Twitter user ID strings."""
    ids = []
    for name in screen_names:
        try:
            user = api.get_user(screen_name=name)
            ids.append(str(user.id))
            print(f'[twitter] Following @{name} (id={user.id}, followers={user.followers_count:,})')
        except tweepy.TweepError as exc:
            print(f'[twitter] WARNING: Could not resolve @{name}: {exc}')
    return ids


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    # Validate credentials
    missing = [k for k in (
        'TWITTER_API_KEY', 'TWITTER_API_SECRET',
        'TWITTER_ACCESS_TOKEN_KEY', 'TWITTER_ACCESS_TOKEN_SECRET',
    ) if not os.environ.get(k)]
    if missing:
        print(f'[producer] ERROR: Missing env vars: {", ".join(missing)}')
        sys.exit(1)

    # Build auth once — reused across stream reconnects
    auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN_KEY, TWITTER_ACCESS_TOKEN_SECRET)
    api  = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    kafka = KafkaProducer()

    while True:
        follow_accounts, track_keywords, languages = load_config()
        current_mtime = config_mtime()

        if not follow_accounts and not track_keywords:
            print('[producer] No accounts or keywords configured — waiting 15 s…')
            print('           Set TWITTER_FOLLOW_ACCOUNTS / TWITTER_TRACK_KEYWORDS in .env')
            print('           or use the settings panel in the web UI.')
            time.sleep(15)
            continue

        follow_ids = resolve_ids(api, follow_accounts)

        if track_keywords:
            print(f'[twitter] Tracking keywords: {track_keywords}')
        if languages:
            print(f'[twitter] Language filter: {languages}')
        else:
            print('[twitter] No language filter — all languages')

        # stop_event lets the watcher thread signal the listener to disconnect
        stop_event = threading.Event()

        def watch_for_config_change():
            """Poll the config file; set stop_event when it changes."""
            while not stop_event.is_set():
                time.sleep(CONFIG_POLL_INTERVAL)
                if config_mtime() != current_mtime:
                    print('[config] Change detected — reconnecting stream…')
                    stop_event.set()

        watcher = threading.Thread(target=watch_for_config_change, daemon=True)
        watcher.start()

        listener = StreamListener(kafka, stop_event)
        stream   = tweepy.Stream(auth=auth, listener=listener)

        filter_kwargs = {}
        if follow_ids:     filter_kwargs['follow']    = follow_ids
        if track_keywords: filter_kwargs['track']     = track_keywords
        if languages:      filter_kwargs['languages'] = languages

        print(f'[twitter] Starting stream…')
        try:
            stream.filter(**filter_kwargs, is_async=False)
        except Exception as exc:
            print(f'[twitter] Stream error: {exc}')

        # Ensure watcher exits cleanly before we loop
        stop_event.set()
        print('[twitter] Stream stopped — reloading config…')
        time.sleep(2)


if __name__ == '__main__':
    main()
