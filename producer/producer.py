#!/usr/bin/env python3
"""
Twitter API v2 streaming producer for robs-twitter.

Uses tweepy 4.x with the v2 FilteredStream endpoint (real-time), falling back
to v2 Recent Search polling if streaming is unavailable on the current tier.

Required env vars
─────────────────
TWITTER_BEARER_TOKEN         App-only bearer token (from Developer Portal)

Optional env vars
─────────────────
TWITTER_API_KEY / TWITTER_API_SECRET          (OAuth 1.0a — not used for streaming)
TWITTER_ACCESS_TOKEN_KEY / TWITTER_ACCESS_TOKEN_SECRET

BROKER_URL              Kafka broker     (default: localhost:9092)
TOPIC                   Kafka topic      (default: twitterTopic)
CONFIG_PATH             Shared config    (default: /config/stream-config.json)
CONFIG_POLL_INTERVAL    Seconds between config-change checks  (default: 15)
POLL_INTERVAL           Seconds between search polls (fallback mode, default: 30)
"""

import json
import os
import sys
import threading
import time

import tweepy
from confluent_kafka import Producer as KafkaProducerClient


# ── Environment ───────────────────────────────────────────────────────────────

def _csv(key, default=''):
    return [s.strip() for s in os.environ.get(key, default).split(',') if s.strip()]

BEARER_TOKEN         = os.environ.get('TWITTER_BEARER_TOKEN', '')
BROKER_URL           = os.environ.get('BROKER_URL', 'localhost:9092')
TOPIC                = os.environ.get('TOPIC', 'twitterTopic')
CONFIG_PATH          = os.environ.get('CONFIG_PATH', '/config/stream-config.json')
CONFIG_POLL_INTERVAL = int(os.environ.get('CONFIG_POLL_INTERVAL', '15'))
POLL_INTERVAL        = int(os.environ.get('POLL_INTERVAL', '30'))

# Env-var fallbacks used before the UI writes a config file
ENV_FOLLOW_ACCOUNTS  = _csv('TWITTER_FOLLOW_ACCOUNTS')
ENV_TRACK_KEYWORDS   = _csv('TWITTER_TRACK_KEYWORDS')
ENV_LANGUAGES        = _csv('TWITTER_LANGUAGES')

TWEET_FIELDS = ['created_at', 'public_metrics', 'entities', 'lang', 'author_id', 'text']
USER_FIELDS  = ['name', 'username', 'profile_image_url', 'verified', 'public_metrics']
EXPANSIONS   = ['author_id']


# ── Config file ───────────────────────────────────────────────────────────────

def load_config():
    try:
        with open(CONFIG_PATH) as f:
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


# ── Tweet normalisation ───────────────────────────────────────────────────────

def normalize_tweet(tweet, includes):
    """
    Convert a tweepy v2 Response tweet + includes into the dict shape the
    frontend expects (mirrors the key fields of a Twitter v1.1 tweet object).
    """
    users   = {u.id: u for u in (includes.get('users') or [])}
    author  = users.get(tweet.author_id)
    metrics = tweet.public_metrics or {}

    # Hashtag entities
    ents     = tweet.entities or {}
    hashtags = [{'text': h.tag} for h in (getattr(ents, 'hashtags', None) or [])]

    # Author public metrics (followers etc.)
    u_metrics = {}
    if author and getattr(author, 'public_metrics', None):
        u_metrics = author.public_metrics

    created = ''
    if tweet.created_at:
        # Match the v1.1 date format the frontend already parses
        created = tweet.created_at.strftime('%a %b %d %H:%M:%S +0000 %Y')

    return {
        'id_str':  str(tweet.id),
        'text':    tweet.text,
        'user': {
            'name':                    getattr(author, 'name',     'Unknown') if author else 'Unknown',
            'screen_name':             getattr(author, 'username', 'unknown') if author else 'unknown',
            'profile_image_url_https': getattr(author, 'profile_image_url', None) if author else None,
            'verified':                getattr(author, 'verified', False) if author else False,
            'followers_count':         u_metrics.get('followers_count', 0),
        },
        'created_at':    created,
        'retweet_count': metrics.get('retweet_count', 0),
        'favorite_count': metrics.get('like_count', 0),
        'entities':      {'hashtags': hashtags},
        'lang':          getattr(tweet, 'lang', '') or '',
    }


# ── Kafka producer ────────────────────────────────────────────────────────────

class KafkaProducer:
    def __init__(self):
        self._p = KafkaProducerClient({'bootstrap.servers': BROKER_URL})
        print(f'[kafka]   Connected to {BROKER_URL!r}')

    def send(self, data: str):
        self._p.poll(0)
        self._p.produce(TOPIC, data.encode('utf-8'), callback=self._report)

    @staticmethod
    def _report(err, msg):
        if err:
            print(f'[kafka]   Delivery failed: {err}')


# ── v2 Filtered Stream ────────────────────────────────────────────────────────

def build_stream_rules(follow_accounts, track_keywords):
    """
    Build a minimal list of tweepy.StreamRule objects.
    Twitter v2 rule operators: https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule
    """
    rules = []
    if follow_accounts:
        # "from:A OR from:B" — matches tweets posted by any of these accounts
        account_expr = ' OR '.join(f'from:{a}' for a in follow_accounts)
        rules.append(tweepy.StreamRule(value=account_expr, tag='accounts'))
    if track_keywords:
        keyword_expr = ' OR '.join(track_keywords)
        rules.append(tweepy.StreamRule(value=keyword_expr, tag='keywords'))
    return rules


def sync_rules(streaming_client, new_rules):
    """Replace all existing stream rules with new_rules."""
    existing = streaming_client.get_rules()
    if existing.data:
        ids = [r.id for r in existing.data]
        streaming_client.delete_rules(ids)
        print(f'[twitter] Deleted {len(ids)} old stream rule(s)')
    if new_rules:
        resp = streaming_client.add_rules(new_rules)
        if resp.errors:
            for e in resp.errors:
                print(f'[twitter] Rule error: {e}')
        else:
            for r in (resp.data or []):
                print(f'[twitter] Rule added: "{r.value}"')


class StreamListenerV2(tweepy.StreamingClient):
    def __init__(self, bearer_token, kafka, stop_event):
        super().__init__(bearer_token, wait_on_rate_limit=True)
        self._kafka     = kafka
        self._stop      = stop_event
        self._count     = 0

    def on_response(self, response):
        if self._stop.is_set():
            self.disconnect()
            return
        try:
            tweet    = response.data
            includes = response.includes or {}
            norm     = normalize_tweet(tweet, includes)
            self._kafka.send(json.dumps(norm))
            self._count += 1
            if self._count % 10 == 0:
                print(f'[twitter] {self._count} tweets forwarded to Kafka')
        except Exception as exc:
            print(f'[producer] Error processing tweet: {exc}')

    def on_errors(self, errors):
        for e in errors:
            print(f'[twitter] Stream error: {e}')

    def on_connection_error(self):
        print('[twitter] Connection error — will retry')

    def on_disconnect(self):
        print('[twitter] Disconnected')


# ── v2 Recent Search polling (fallback) ───────────────────────────────────────

def build_search_query(follow_accounts, track_keywords, languages):
    parts = []
    if follow_accounts:
        parts.append('(' + ' OR '.join(f'from:{a}' for a in follow_accounts) + ')')
    if track_keywords:
        parts.append('(' + ' OR '.join(track_keywords) + ')')
    query = ' OR '.join(parts) if parts else ''
    if languages and query:
        lang_expr = ' OR '.join(f'lang:{l}' for l in languages)
        query += f' ({lang_expr})'
    return query


def run_polling(client, kafka, stop_event, follow_accounts, track_keywords, languages):
    """Poll the v2 recent search endpoint periodically."""
    query   = build_search_query(follow_accounts, track_keywords, languages)
    last_id = None
    print(f'[twitter] Polling mode — query: "{query}" every {POLL_INTERVAL}s')

    while not stop_event.is_set():
        try:
            kwargs = dict(
                query        = query,
                max_results  = 10,
                tweet_fields = TWEET_FIELDS,
                expansions   = EXPANSIONS,
                user_fields  = USER_FIELDS,
            )
            if last_id:
                kwargs['since_id'] = last_id

            resp = client.search_recent_tweets(**kwargs)

            if resp.data:
                # Process oldest first so the feed renders in chronological order
                includes = resp.includes or {}
                for tweet in reversed(resp.data):
                    norm = normalize_tweet(tweet, includes)
                    kafka.send(json.dumps(norm))
                last_id = resp.data[0].id
                print(f'[twitter] Polled {len(resp.data)} tweet(s)')

        except tweepy.TweepyException as exc:
            print(f'[twitter] Poll error: {exc}')

        # Sleep in small increments so stop_event is checked promptly
        for _ in range(POLL_INTERVAL):
            if stop_event.is_set():
                break
            time.sleep(1)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    if not BEARER_TOKEN:
        print('[producer] ERROR: TWITTER_BEARER_TOKEN is not set.')
        print('           Get it from https://developer.twitter.com/en/portal')
        print('           under your app → "Keys and Tokens" → Bearer Token')
        sys.exit(1)

    kafka  = KafkaProducer()
    client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

    # Probe whether filtered streaming is available on this tier
    print('[twitter] Checking filtered stream access…')
    streaming_available = False
    try:
        probe = tweepy.StreamingClient(BEARER_TOKEN)
        probe.get_rules()           # simple read — available on Basic+
        streaming_available = True
        print('[twitter] Filtered stream: available ✓')
    except tweepy.errors.Forbidden:
        print('[twitter] Filtered stream: not available on this tier')
        print('[twitter] Falling back to Recent Search polling')
    except tweepy.errors.Unauthorized:
        print('[producer] ERROR: Bearer token is invalid or expired.')
        sys.exit(1)
    except Exception as exc:
        print(f'[twitter] Stream probe error: {exc} — trying polling mode')

    while True:
        follow_accounts, track_keywords, languages = load_config()
        current_mtime = config_mtime()

        if not follow_accounts and not track_keywords:
            print('[producer] No accounts or keywords configured — waiting 15 s…')
            print('           Use the settings panel in the web UI to add some.')
            time.sleep(15)
            continue

        stop_event = threading.Event()

        # Watcher thread: signals stop_event when config file changes
        def watch_config():
            while not stop_event.is_set():
                time.sleep(CONFIG_POLL_INTERVAL)
                if config_mtime() != current_mtime:
                    print('[config] Change detected — reconnecting stream…')
                    stop_event.set()

        watcher = threading.Thread(target=watch_config, daemon=True)
        watcher.start()

        if streaming_available:
            stream = StreamListenerV2(BEARER_TOKEN, kafka, stop_event)
            rules  = build_stream_rules(follow_accounts, track_keywords)
            sync_rules(stream, rules)

            print('[twitter] Starting v2 filtered stream…')
            try:
                stream.filter(
                    tweet_fields = TWEET_FIELDS,
                    expansions   = EXPANSIONS,
                    user_fields  = USER_FIELDS,
                    threaded     = False,
                )
            except Exception as exc:
                print(f'[twitter] Stream error: {exc}')
        else:
            # Polling mode
            run_polling(client, kafka, stop_event, follow_accounts, track_keywords, languages)

        stop_event.set()
        print('[twitter] Stopped — reloading config in 2 s…')
        time.sleep(2)


if __name__ == '__main__':
    main()
