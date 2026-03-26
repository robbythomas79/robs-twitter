#!/usr/bin/env python3
"""
Run this inside the twitter-producer container to diagnose API issues:

  docker compose exec twitter-producer python test_credentials.py

It checks credentials, access level, and whether the streaming API is reachable.
"""

import os
import sys

print("=" * 60)
print("Twitter API Credential & Access Diagnostic")
print("=" * 60)

# ── 1. Check env vars are present ──────────────────────────────
required = {
    "TWITTER_API_KEY":             os.environ.get("TWITTER_API_KEY", ""),
    "TWITTER_API_SECRET":          os.environ.get("TWITTER_API_SECRET", ""),
    "TWITTER_ACCESS_TOKEN_KEY":    os.environ.get("TWITTER_ACCESS_TOKEN_KEY", ""),
    "TWITTER_ACCESS_TOKEN_SECRET": os.environ.get("TWITTER_ACCESS_TOKEN_SECRET", ""),
}

print("\n[1] Environment variables:")
all_present = True
for key, val in required.items():
    if val:
        print(f"    ✓  {key} = {val[:6]}…{'*' * 10}")
    else:
        print(f"    ✗  {key} = NOT SET")
        all_present = False

if not all_present:
    print("\n❌  Missing credentials. Copy .env.example → .env and fill in your API keys.")
    sys.exit(1)

# ── 2. Try to import tweepy ─────────────────────────────────────
print("\n[2] Importing tweepy…")
try:
    import tweepy
    print(f"    ✓  tweepy {tweepy.__version__} imported")
except ImportError as e:
    print(f"    ✗  {e}")
    sys.exit(1)

# ── 3. Authenticate ─────────────────────────────────────────────
print("\n[3] Authenticating with Twitter API…")
try:
    auth = tweepy.OAuthHandler(required["TWITTER_API_KEY"], required["TWITTER_API_SECRET"])
    auth.set_access_token(required["TWITTER_ACCESS_TOKEN_KEY"], required["TWITTER_ACCESS_TOKEN_SECRET"])
    api = tweepy.API(auth)
    me = api.verify_credentials()
    print(f"    ✓  Authenticated as @{me.screen_name} (id={me.id})")
except tweepy.TweepError as e:
    print(f"    ✗  Auth failed: {e}")
    if "401" in str(e):
        print("       → Your API key/secret or access token/secret is wrong.")
        print("         Regenerate them at https://developer.twitter.com/en/portal")
    elif "403" in str(e):
        print("       → Your app does not have permission for this action.")
        print("         Check your app's permissions in the Developer Portal.")
    sys.exit(1)

# ── 4. Rate limit / app context check ──────────────────────────
print("\n[4] Checking API rate limits (confirms app permissions)…")
try:
    limits = api.rate_limit_status(resources="statuses")
    tl = limits["resources"]["statuses"].get("/statuses/home_timeline", {})
    print(f"    ✓  Rate limit check passed")
    print(f"       /statuses/home_timeline: {tl.get('remaining','?')}/{tl.get('limit','?')} remaining")
except tweepy.TweepError as e:
    print(f"    ✗  {e}")

# ── 5. Resolve a test account ───────────────────────────────────
print("\n[5] Resolving a test account (@Twitter)…")
try:
    user = api.get_user(screen_name="Twitter")
    print(f"    ✓  Resolved @Twitter → id={user.id}")
except tweepy.TweepError as e:
    print(f"    ✗  {e}")
    if "404" in str(e):
        print("       → Account not found (might be suspended/renamed). This is fine.")
    elif "401" in str(e) or "403" in str(e):
        print("       → Your access level cannot look up users.")
        print("         Streaming requires at least Basic ($100/mo) API access.")

# ── 6. Attempt a minimal stream connection ──────────────────────
print("\n[6] Testing streaming API access (5 second probe)…")
print("    Connecting to filtered stream endpoint…")

import threading

result = {"connected": False, "error": None}

class Probe(tweepy.StreamListener):
    def on_connect(self):
        result["connected"] = True
    def on_error(self, status):
        result["error"] = status
        return False
    def on_timeout(self):
        return False

try:
    probe_stream = tweepy.Stream(auth=auth, listener=Probe())
    t = threading.Thread(
        target=probe_stream.filter,
        kwargs={"track": ["the"], "is_async": False},
        daemon=True
    )
    t.start()
    t.join(timeout=5)
    probe_stream.disconnect()

    if result["connected"]:
        print("    ✓  Streaming API is accessible — tweets should flow!")
    elif result["error"]:
        code = result["error"]
        print(f"    ✗  Stream returned HTTP {code}")
        if code == 401:
            print("       → Invalid credentials for the streaming endpoint.")
        elif code == 403:
            print("       → Your API access level does not include filtered streaming.")
            print("         Free tier (v2) does NOT support v1.1 filtered stream.")
            print("         You need Twitter API Basic ($100/mo) or higher.")
            print("         See: https://developer.twitter.com/en/products/twitter-api")
        elif code == 420 or code == 429:
            print("       → Rate limited. Wait a few minutes and try again.")
        elif code == 406:
            print("       → Invalid stream filter parameters.")
    else:
        print("    ⚠  No response in 5 s — stream may be connecting slowly.")
        print("       Check producer logs: docker compose logs -f twitter-producer")
except Exception as e:
    print(f"    ✗  Exception: {e}")

print("\n" + "=" * 60)
print("Diagnostic complete.")
print("=" * 60)
