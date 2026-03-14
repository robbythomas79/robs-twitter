'use strict';

// ── Helpers ──────────────────────────────────────────────────────────────────

function escapeHTML(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatNumber(n) {
  if (!n || isNaN(n)) return '0';
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace('.0', '') + 'M';
  if (n >= 1_000)     return (n / 1_000).toFixed(1).replace('.0', '') + 'K';
  return String(n);
}

// Parse the Twitter API v1.1 date "Mon Jan 01 00:00:00 +0000 2024"
function parseTwitterDate(str) {
  if (!str) return null;
  try { return new Date(str); } catch { return null; }
}

function timeAgo(date) {
  if (!date) return '';
  const diff = (Date.now() - date.getTime()) / 1000;
  if (diff < 60)   return `${Math.floor(diff)}s`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400)return `${Math.floor(diff / 3600)}h`;
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

// Highlight #hashtags, @mentions, and URLs in tweet text
function formatText(text, hashtags) {
  const escaped = escapeHTML(text);
  return escaped
    .replace(/(https?:\/\/[^\s]+)/g, '<span class="url">$1</span>')
    .replace(/@(\w+)/g, '<span class="mention">@$1</span>')
    .replace(/#(\w+)/g, '<span class="hashtag">#$1</span>');
}

// Retweet SVG icon
const ICON_RT = `<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
  <path d="M4.5 3.88l4.432 4.14-1.364 1.46L5.5 7.55V16c0 1.1.896 2 2 2H13v2H7.5c-2.209 0-4-1.79-4-4V7.55L1.432 9.48.068 8.02 4.5 3.88zM16.5 6H11V4h5.5c2.209 0 4 1.79 4 4v8.45l2.068-1.93 1.364 1.46-4.432 4.14-4.432-4.14 1.364-1.46 2.068 1.93V8c0-1.1-.896-2-2-2z"/>
</svg>`;

// Heart SVG icon
const ICON_HEART = `<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
  <path d="M20.884 13.19c-1.351 2.48-4.001 5.12-8.379 7.67l-.503.3-.504-.3c-4.379-2.55-7.029-5.19-8.382-7.67-1.36-2.5-1.41-4.86-.514-6.67.887-1.79 2.647-2.91 4.601-3.01 1.651-.09 3.368.56 4.798 2.01 1.429-1.45 3.146-2.1 4.796-2.01 1.954.1 3.714 1.22 4.601 3.01.896 1.81.846 4.17-.514 6.67z"/>
</svg>`;

// ── Tweet parser ──────────────────────────────────────────────────────────────

function parseTweet(raw) {
  const isRT = Boolean(raw.retweeted_status);
  // Use the original tweet for content; raw for user/time
  const src = isRT ? raw.retweeted_status : raw;

  const text =
    src.extended_tweet?.full_text ||
    src.full_text ||
    src.text ||
    '';

  return {
    id:          raw.id_str || String(Date.now()),
    text,
    user: {
      name:       raw.user?.name        || 'Unknown',
      screenName: raw.user?.screen_name || 'unknown',
      avatar:     raw.user?.profile_image_url_https || null,
      verified:   Boolean(raw.user?.verified),
      followers:  raw.user?.followers_count || 0,
    },
    createdAt:  parseTwitterDate(raw.created_at),
    retweets:   src.retweet_count   || 0,
    likes:      src.favorite_count  || 0,
    hashtags:   src.entities?.hashtags?.map(h => h.text.toLowerCase()) || [],
    lang:       raw.lang || '',
    isRT,
    rtBy:       isRT ? raw.user?.screen_name : null,
  };
}

// ── Card builder ─────────────────────────────────────────────────────────────

function buildCard(tweet) {
  const el = document.createElement('article');
  el.className = 'tweet-card';
  el.dataset.id = tweet.id;

  const avatarHTML = tweet.user.avatar
    ? `<img src="${escapeHTML(tweet.user.avatar)}"
            alt="${escapeHTML(tweet.user.name)}'s avatar"
            onerror="this.outerHTML='<div class=\\'avatar-fallback\\'>${escapeHTML(tweet.user.name[0] || '?')}</div>'">`
    : `<div class="avatar-fallback">${escapeHTML((tweet.user.name[0] || '?').toUpperCase())}</div>`;

  const rtLabel = tweet.isRT
    ? `<div class="rt-label">${ICON_RT} <span>${escapeHTML(tweet.rtBy)} retweeted</span></div>`
    : '';

  const verifiedBadge = tweet.user.verified
    ? `<span class="verified-badge" title="Verified">✓</span>`
    : '';

  const langBadge = tweet.lang && tweet.lang !== 'und'
    ? `<span class="lang-badge">${escapeHTML(tweet.lang)}</span>`
    : '';

  el.innerHTML = `
    ${rtLabel}
    <div class="tweet-body">
      <div class="tweet-avatar">${avatarHTML}</div>
      <div class="tweet-content">
        <div class="tweet-header">
          <span class="user-name" title="${escapeHTML(tweet.user.name)}">${escapeHTML(tweet.user.name)}</span>
          ${verifiedBadge}
          <span class="user-handle">@${escapeHTML(tweet.user.screenName)}</span>
          <span class="tweet-sep">·</span>
          <time class="tweet-time" datetime="${tweet.createdAt?.toISOString() || ''}">${timeAgo(tweet.createdAt)}</time>
        </div>
        <div class="tweet-text">${formatText(tweet.text)}</div>
        <div class="tweet-footer">
          <span class="metric metric-rt" title="Retweets">
            ${ICON_RT} ${formatNumber(tweet.retweets)}
          </span>
          <span class="metric metric-like" title="Likes">
            ${ICON_HEART} ${formatNumber(tweet.likes)}
          </span>
          ${langBadge}
        </div>
      </div>
    </div>`;

  // Clicking a hashtag in the text sets the filter
  el.querySelectorAll('.hashtag').forEach((span) => {
    span.addEventListener('click', () => {
      const tag = span.textContent.slice(1); // strip leading #
      app.setFilter(tag);
    });
  });

  return el;
}

// ── Main application ──────────────────────────────────────────────────────────

class App {
  constructor() {
    // State
    this.tweets      = [];   // parsed tweet objects (newest first)
    this.totalCount  = 0;
    this.filter      = '';
    this.hashtags    = {};   // tag → count
    this.langCounts  = {};
    this.rateWindow  = [];   // timestamps for rate calculation
    this.pendingWhileScrolled = 0;

    // Reconnect
    this.ws              = null;
    this.reconnectDelay  = 1000;

    // DOM refs
    this.feedEl        = document.getElementById('feed');
    this.emptyState    = document.getElementById('emptyState');
    this.statusDot     = document.getElementById('statusDot');
    this.statusLabel   = document.getElementById('statusLabel');
    this.statTotal     = document.getElementById('statTotal');
    this.statRate      = document.getElementById('statRate');
    this.statLang      = document.getElementById('statLang');
    this.filterInput   = document.getElementById('filterInput');
    this.filterHint    = document.getElementById('filterHint');
    this.hashtagList   = document.getElementById('hashtagList');
    this.newBanner     = document.getElementById('newTweetsBanner');
    this.themeBtn      = document.getElementById('themeBtn');
    this.themeIcon     = document.getElementById('themeIcon');

    this.bindEvents();
    this.connect();
    setInterval(() => this.updateRate(), 5000);
  }

  // ── Event bindings ──

  bindEvents() {
    this.filterInput.addEventListener('input', () => {
      this.filter = this.filterInput.value.trim().toLowerCase();
      this.filterHint.textContent = this.filter
        ? `Showing tweets matching "${this.filter}"`
        : '';
      this.rerenderFeed();
    });

    this.themeBtn.addEventListener('click', () => this.toggleTheme());

    this.newBanner.addEventListener('click', () => {
      this.pendingWhileScrolled = 0;
      this.newBanner.hidden = true;
      this.rerenderFeed();
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    window.addEventListener('scroll', () => {
      if (window.scrollY < 100 && this.pendingWhileScrolled > 0) {
        this.pendingWhileScrolled = 0;
        this.newBanner.hidden = true;
        this.rerenderFeed();
      }
    });
  }

  // ── WebSocket ──

  connect() {
    this.setStatus('connecting', 'Connecting…');
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.ws = new WebSocket(`${proto}//${location.host}/ws`);

    this.ws.addEventListener('open', () => {
      this.setStatus('connected', 'Live');
      this.reconnectDelay = 1000;
    });

    this.ws.addEventListener('message', ({ data }) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'tweet')   this.onTweet(msg.tweet);
        if (msg.type === 'history') msg.tweets.forEach(t => this.ingestTweet(t, false));
      } catch { /* ignore malformed */ }
    });

    this.ws.addEventListener('close', () => {
      this.setStatus('disconnected', `Reconnecting in ${this.reconnectDelay / 1000}s…`);
      setTimeout(() => this.connect(), this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30_000);
    });

    this.ws.addEventListener('error', () => {
      this.setStatus('disconnected', 'Connection error');
    });
  }

  // ── Incoming tweet handling ──

  onTweet(raw) {
    this.ingestTweet(raw, true);
  }

  ingestTweet(raw, isNew) {
    const tweet = parseTweet(raw);

    // Deduplicate by id
    if (this.tweets.some(t => t.id === tweet.id)) return;

    this.tweets.unshift(tweet);
    if (this.tweets.length > 500) this.tweets.pop();

    this.totalCount++;
    this.rateWindow.push(Date.now());

    // Update hashtag counters
    tweet.hashtags.forEach(tag => {
      this.hashtags[tag] = (this.hashtags[tag] || 0) + 1;
    });

    // Update language counters
    if (tweet.lang && tweet.lang !== 'und') {
      this.langCounts[tweet.lang] = (this.langCounts[tweet.lang] || 0) + 1;
    }

    this.updateStats();
    this.updateHashtagList();

    if (isNew && this.matchesFilter(tweet)) {
      if (window.scrollY > 200) {
        // User scrolled down – show banner instead
        this.pendingWhileScrolled++;
        this.newBanner.textContent = `↑ Show ${this.pendingWhileScrolled} new tweet${this.pendingWhileScrolled > 1 ? 's' : ''}`;
        this.newBanner.hidden = false;
      } else {
        this.prependCard(tweet);
      }
    } else if (!isNew && this.matchesFilter(tweet)) {
      this.appendCard(tweet);
    }
  }

  // ── Filter ──

  matchesFilter(tweet) {
    if (!this.filter) return true;
    const q = this.filter.replace(/^[#@]/, '');
    const fullText = `${tweet.text} ${tweet.user.screenName} ${tweet.user.name} ${tweet.hashtags.join(' ')}`.toLowerCase();
    return fullText.includes(q);
  }

  setFilter(value) {
    this.filterInput.value = value;
    this.filter = value.toLowerCase();
    this.filterHint.textContent = `Showing tweets matching "${value}"`;
    this.rerenderFeed();
  }

  // ── DOM manipulation ──

  prependCard(tweet) {
    this.hideEmptyState();
    const card = buildCard(tweet);
    this.feedEl.insertBefore(card, this.feedEl.firstChild);
  }

  appendCard(tweet) {
    this.hideEmptyState();
    const card = buildCard(tweet);
    this.feedEl.appendChild(card);
  }

  hideEmptyState() {
    if (this.emptyState && this.emptyState.parentNode === this.feedEl) {
      this.feedEl.removeChild(this.emptyState);
    }
  }

  rerenderFeed() {
    this.feedEl.innerHTML = '';
    const filtered = this.tweets.filter(t => this.matchesFilter(t));
    if (filtered.length === 0) {
      this.feedEl.appendChild(this.emptyState);
      this.emptyState.querySelector('p').textContent =
        this.filter ? 'No tweets match your filter.' : 'Waiting for live tweets…';
      return;
    }
    const frag = document.createDocumentFragment();
    filtered.forEach(t => frag.appendChild(buildCard(t)));
    this.feedEl.appendChild(frag);
  }

  // ── Stats ──

  updateStats() {
    this.statTotal.textContent = this.totalCount.toLocaleString();
  }

  updateRate() {
    const cutoff = Date.now() - 60_000;
    this.rateWindow = this.rateWindow.filter(ts => ts > cutoff);
    this.statRate.textContent = this.rateWindow.length;

    // Top language
    const sorted = Object.entries(this.langCounts).sort((a, b) => b[1] - a[1]);
    this.statLang.textContent = sorted.length ? sorted[0][0].toUpperCase() : '—';
  }

  updateHashtagList() {
    const top = Object.entries(this.hashtags)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8);

    if (top.length === 0) return;

    this.hashtagList.innerHTML = top
      .map(([tag, count]) =>
        `<li class="hashtag-item">
           <span class="hashtag-tag">#${escapeHTML(tag)}</span>
           <span class="hashtag-count">${count}</span>
         </li>`)
      .join('');

    this.hashtagList.querySelectorAll('.hashtag-item').forEach((li, i) => {
      li.addEventListener('click', () => this.setFilter(top[i][0]));
      li.style.cursor = 'pointer';
    });
  }

  // ── UI helpers ──

  setStatus(state, label) {
    this.statusDot.className = `status-dot ${state}`;
    this.statusLabel.textContent = label;
  }

  toggleTheme() {
    const html = document.documentElement;
    const isDark = html.getAttribute('data-theme') === 'dark';
    html.setAttribute('data-theme', isDark ? 'light' : 'dark');
    this.themeIcon.textContent = isDark ? '🌙' : '☀️';
  }
}

// ── Boot ─────────────────────────────────────────────────────────────────────
const app = new App();
