#!/usr/bin/env python3
"""
RSS Feed Processor — Bangla Editorial Pipeline

Feeds: bdit/daily_feed.xml + bdit/daily_feed_2.xml
Only Bengali-script titles are classified. Non-Bangla titles are skipped entirely.

Outputs:
  curated_feed_bdit.xml
Stats:
  fetch_stats_bdit.json
"""

import feedparser
import json
import os
import time
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from mistralai.client import Mistral
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse

import requests

try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# -- FEEDS ---------------------------------------------------------------------

FEED_URLS = [
    "https://evilgodfahim.github.io/bdit/daily_feed.xml",
    "https://evilgodfahim.github.io/bdit/daily_feed_2.xml",
]

EXISTING_API_FEEDS = set(FEED_URLS)
KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

MISTRAL_MODEL = "mistral-medium-latest"
PROCESSED_FILE = "processed_articles_bdit.json"
SELECTED_FILE = "selected_articles_bdit.json"
OUTPUT_XML = "curated_feed_bdit.xml"
STATS_FILE = "fetch_stats_bdit.json"
MAX_ARTICLES_PER_FEED = 100
MAX_AGE_HOURS = 26
ALLOW_MISSING_DATES = True
ALLOW_OLDER = False
MAX_FEED_ITEMS = 500
RETENTION_DAYS = 10

# -- BANGLA FILTER -------------------------------------------------------------

def is_bangla_title(title: str) -> bool:
    """Return True only if the title contains ≥4 Bengali-script characters."""
    if not title:
        return False
    return sum(1 for c in title if "\u0980" <= c <= "\u09FF") >= 4

# -- PROMPT --------------------------------------------------------------------

BANGLA_PROMPT = """You are a strict editorial classifier for a Bangladeshi Bengali-language opinion journalism feed.

Input: numbered Bengali editorial and op-ed titles from Bangladeshi newspapers.
Task: classify each as SIGNAL or NOISE. Output only SIGNAL indices.

DEFAULT: NOISE. Only promote to SIGNAL when the evidence is clear and strong.
A title earns SIGNAL only when a serious editor would call it analytically substantive AND nationally or globally consequential.

════════════════════════════════════════════════════════════
STEP 1 — INSTANT NOISE
════════════════════════════════════════════════════════════

Classify NOISE immediately if the title fits any pattern below.
Do not proceed to Step 2.

▸ TRIBUTES & COMMEMORATION
  A piece honoring, remembering, or mourning any person, or marking a historical date.
  Trigger words: স্মরণ, শ্রদ্ধাঞ্জলি, প্রয়াণ, জন্মদিন, জন্মবার্ষিকী, মৃত্যুবার্ষিকী, স্মৃতিচারণ, চিরবিদায়

▸ PARTISAN / IDEOLOGICAL
  Praises or attacks a party, leader, or government without naming a concrete, specific policy failure.
  Defending or attacking a political figure's character, legacy, vision, or ideology is always NOISE.
  Trigger words: দলীয়, অঙ্গীকার, শপথ (in aspirational context), মহান নেতা, গণতান্ত্রিক শক্তি

▸ CULTURAL / INSPIRATIONAL / MORAL
  General moral, religious, or patriotic encouragement. Seasonal, festival, or anniversary reflections.
  Vague calls to action with no named policy domain.
  Examples: "এগিয়ে যেতে হবে", "স্বপ্নের বাংলাদেশ", "আলোর পথে", "জেগে ওঠো", "ঐক্যই শক্তি"
  Trigger events: ঈদ, পূজা, পহেলা বৈশাখ, একুশে, বিজয় দিবস, স্বাধীনতা দিবস (unless title directly engages a policy dimension)

▸ LOCAL / MICRO-SCALE
  Concerns a single district, upazila, school, hospital, or local figure with no stated national implication.

▸ SPORTS / ENTERTAINMENT / LIFESTYLE
  Cricket, football, film, celebrity, food, fashion, travel.

▸ VAGUE / UNNAMED DOMAIN
  Uses words like সংকট, সমস্যা, চ্যালেঞ্জ, বিপদ without specifying what kind.
  A title that could have been written about any country, any year, or any context is NOISE.
  "কবে মিলবে মুক্তি", "এই অবস্থা কতদিন চলবে", "দেশের হাল কোথায়" — always NOISE.

════════════════════════════════════════════════════════════
STEP 2 — SIGNAL CRITERIA (both conditions required)
════════════════════════════════════════════════════════════

A title is SIGNAL only if it satisfies BOTH A and B.

A) NAMES OR CLEARLY IMPLIES a specific domain from this list:

  ECONOMICS & FINANCE
  · Macroeconomics: inflation, foreign reserves, exchange rate, budget deficit
  · Banking: non-performing loans (খেলাপি ঋণ), financial fraud, banking reform
  · Trade: export performance, import duties, trade agreements, tariffs
  · Investment: FDI, SEZ, industrial policy, business climate
  · Labour: wage policy, remittances, industrial safety, workers' rights
  · Agriculture: food security, crop prices, subsidy, irrigation policy

  GOVERNANCE & INSTITUTIONS
  · Systemic corruption with a named sector (customs, health, land, police)
  · Public administration failure explicitly at national scale
  · Constitutional matters, electoral reform, judicial independence
  · Press freedom, digital rights, surveillance laws, cyber regulations

  PUBLIC SYSTEMS (national-scale framing required)
  · Healthcare system: capacity, policy, equity — NOT a single hospital
  · Education system: curriculum, quality, policy — NOT a single school
  · Infrastructure: roads, bridges, seaports, power grid
  · Energy: power crisis, fuel pricing, load-shedding, renewable policy

  ENVIRONMENT
  · Climate impact on Bangladesh (floods, salinity, coastal erosion, displacement)
  · River rights (Padma, Meghna, Brahmaputra, Teesta)
  · Industrial pollution at national scale
  · Disaster preparedness policy

  FOREIGN AFFAIRS (Bangladesh-specific)
  · Water-sharing: Teesta, Ganges, Brahmaputra
  · Bilateral trade or investment: India, China, USA, EU, Middle East
  · Migration diplomacy, remittance policy
  · Rohingya crisis: repatriation, diplomacy, international burden-sharing
  · Bangladesh in multilateral bodies: UN, WTO, SAARC, OIC

  GLOBAL AFFAIRS (significant events only — not every foreign news item)
  · Ongoing interstate wars or major military escalations (not minor skirmishes)
  · Global economic crises, major sanctions regimes, trade wars
  · International climate agreements with binding consequences
  · Humanitarian catastrophes affecting 100,000+ people
  · Great-power competition (US-China, Russia-NATO) with explicit global consequences

B) HAS NATIONAL OR GLOBAL SCALE
  The domain must operate at national policy level or above.
  A title about one city's flooding is NOISE; a title about Bangladesh's flood preparedness policy is SIGNAL.

════════════════════════════════════════════════════════════
STEP 3 — DEDUPLICATION (apply to SIGNAL set only)
════════════════════════════════════════════════════════════

Among items already classified SIGNAL:
· Remove near-duplicates covering the same event or story
· Keep only the lowest index for each duplicate group

════════════════════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════════════════════

{{"signal": [0-based indices]}}
Valid JSON only. No markdown. No commentary. No explanation.

════════════════════════════════════════════════════════════
EXAMPLES — study the reasoning, not just the output
════════════════════════════════════════════════════════════

EXAMPLE 1 — Standard batch:

Input:
0. বাংলাদেশের বিদ্যুৎ সংকট কি স্থায়ী সমাধানের পথে     ← SIGNAL: energy policy, national scale, specific domain
1. আমাদের এগিয়ে যেতে হবে                               ← NOISE: vague aspiration, zero domain named
2. তিস্তার পানি ও বাংলাদেশের কৃষির ভবিষ্যৎ             ← SIGNAL: water rights + agriculture policy, BD-India dimension
3. এক মহান নেতার প্রতি শ্রদ্ধাঞ্জলি                    ← NOISE: tribute (instant NOISE, Step 1)
4. সরকারি হাসপাতালে দুর্নীতি ও জনভোগান্তি              ← SIGNAL: systemic healthcare corruption, national institution
5. স্বপ্নের বাংলাদেশ গড়ার প্রতিশ্রুতি                  ← NOISE: aspirational, no policy domain
6. মূল্যস্ফীতির চাপে সাধারণ মানুষের জীবন               ← SIGNAL: macroeconomics (inflation), named domain, national
7. গাজায় গণহত্যা ও আন্তর্জাতিক আইনের সংকট             ← SIGNAL: major ongoing conflict + international law
8. দলীয় আদর্শই আমাদের পথ দেখাবে                       ← NOISE: partisan ideology, no policy substance
9. শিক্ষাব্যবস্থার সংকট ও করণীয়                        ← SIGNAL: education system, national framing, specific domain
10. জলবায়ু পরিবর্তন ও বৈশ্বিক রাজনীতির নতুন সমীকরণ   ← SIGNAL: global climate + geopolitical consequence
11. বৈদেশিক মুদ্রার রিজার্ভ কমছে, কী করবে বাংলাদেশ    ← SIGNAL: foreign reserves, macroeconomic, BD-specific
12. পোশাক রপ্তানিতে ধস ও অর্থনীতির ঝুঁকি              ← SIGNAL: garment exports + economic risk, named domain

Output: {{"signal": [0, 2, 4, 6, 7, 9, 10, 11, 12]}}

---

EXAMPLE 2 — Tricky borderlines:

Input:
0. গণমাধ্যমের স্বাধীনতা ও রাষ্ট্রের দায়িত্ব            ← SIGNAL: press freedom, governance, national
1. বিজয় দিবসের চেতনায় আলোকিত হোক প্রজন্ম              ← NOISE: commemorative, no policy analysis (instant NOISE)
2. ব্যাংক খাতের খেলাপি ঋণ এবং আর্থিক স্থিতিশীলতা       ← SIGNAL: banking NPLs, financial stability, specific named problem
3. নেতার জন্মদিনে আমাদের অঙ্গীকার                       ← NOISE: tribute + partisan (two instant NOISE triggers)
4. ইউক্রেন যুদ্ধ এবং বিশ্ব খাদ্য নিরাপত্তার ভবিষ্যৎ    ← SIGNAL: interstate war → global food security consequence
5. বাজারে সিন্ডিকেটের দৌরাত্ম্য কতদিন চলবে             ← SIGNAL: market cartel/price manipulation, economic governance
6. ধর্মীয় সম্প্রীতির অনুপ্রেরণায় এগিয়ে চলি           ← NOISE: cultural/inspirational (instant NOISE), no policy
7. স্বাস্থ্যসেবায় বৈষম্য ও রাষ্ট্রের ব্যর্থতা          ← SIGNAL: healthcare equity, explicit state failure, national
8. মার্কিন-চীন বাণিজ্যযুদ্ধ ও বৈশ্বিক অর্থনীতির গতিপথ ← SIGNAL: US-China trade war, major global economic event
9. বাংলাদেশের শিল্পনীতি ও বিনিয়োগ পরিবেশ               ← SIGNAL: industrial policy + investment climate, national
10. একুশের চেতনা ও আমাদের কর্তব্য                       ← NOISE: Language Day commemoration, aspirational (instant NOISE)

Output: {{"signal": [0, 2, 4, 5, 7, 8, 9]}}

---

EXAMPLE 3 — Hard NOISE cases that superficially resemble SIGNAL:

Input:
0. দেশের উন্নয়নে তরুণদের ভূমিকা                        ← NOISE: vague aspiration — "উন্নয়ন" alone is not a domain
1. সংকট থেকে উত্তরণের পথ                                ← NOISE: "সংকট" unnamed — which crisis? Rule: unnamed domain = NOISE
2. একটি জেলার স্বাস্থ্যসেবার করুণ চিত্র                ← NOISE: single district, local scale, no national implication stated
3. নির্বাচন কমিশনের সংস্কার এখন জরুরি                   ← SIGNAL: electoral reform, named institution, governance, national
4. দেশপ্রেমের চেতনায় জাগ্রত হোক বাংলাদেশ               ← NOISE: nationalist inspiration, zero policy domain
5. রোহিঙ্গা সংকট ও বাংলাদেশের কূটনৈতিক চ্যালেঞ্জ       ← SIGNAL: Rohingya + BD diplomacy, named foreign policy domain
6. বৈষম্যহীন সমাজ গড়ার স্বপ্ন                           ← NOISE: abstract ideal, no named policy mechanism or sector
7. কৃষকের ন্যায্য মূল্য ও সরকারের নীতি                  ← SIGNAL: agricultural pricing + explicit state policy, national
8. ক্রিকেটে বাংলাদেশের সাফল্য ও ভবিষ্যৎ                ← NOISE: sports (instant NOISE)
9. শেয়ারবাজারের অস্থিরতা ও বিনিয়োগকারীদের সংকট         ← SIGNAL: stock market instability, financial domain, named
10. আমাদের শিক্ষার মান কেন এত নিচে                       ← SIGNAL: education quality, national system-level question
11. মানবতার ডাকে সাড়া দিন                                ← NOISE: moral appeal, no policy domain whatsoever
12. সুশাসনের পথে বাংলাদেশ                               ← NOISE: "সুশাসন" alone too vague — no specific institution or failure named

Output: {{"signal": [3, 5, 7, 9, 10]}}

Article titles:
{titles}
"""

# -- CONSTANTS -----------------------------------------------------------------

MEDIA_NS = "http://search.yahoo.com/mrss/"
MEDIA_TAG = "{%s}" % MEDIA_NS
ET.register_namespace("media", MEDIA_NS)

BD_TZ = timezone(timedelta(hours=6))

STATS = {
    "per_feed": {},
    "per_method": {"KL": 0, "DIRECT": 0},
    "total_fetched": 0,
    "total_passed_age": 0,
    "total_new": 0,
    "total_bangla": 0,
    "total_skipped_non_bangla": 0,
    "total_signal_mistral": 0,
    "total_signal": 0,
    "total_signal_deduped": 0,
    "timestamp": None,
}

# -- I/O -----------------------------------------------------------------------

def load_processed_articles():
    empty = {
        "article_ids": [],
        "article_links": [],
        "id_timestamps": {},
        "link_timestamps": {},
        "last_updated": None,
    }
    if not Path(PROCESSED_FILE).exists():
        return empty
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return empty

    cutoff = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).isoformat()

    id_ts = {k: v for k, v in data.get("id_timestamps", {}).items() if v >= cutoff}
    link_ts = {k: v for k, v in data.get("link_timestamps", {}).items() if v >= cutoff}

    return {
        "article_ids": list(id_ts.keys()),
        "article_links": list(link_ts.keys()),
        "id_timestamps": id_ts,
        "link_timestamps": link_ts,
        "last_updated": data.get("last_updated"),
    }

def save_processed_articles(data):
    now_iso = datetime.utcnow().isoformat()
    cutoff = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).isoformat()

    existing = {"id_timestamps": {}, "link_timestamps": {}}
    if Path(PROCESSED_FILE).exists():
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    id_ts = existing.get("id_timestamps", {})
    link_ts = existing.get("link_timestamps", {})

    for aid in data.get("article_ids", []):
        if aid and aid not in id_ts:
            id_ts[aid] = now_iso
    for lnk in data.get("article_links", []):
        if lnk and lnk not in link_ts:
            link_ts[lnk] = now_iso

    id_ts = {k: v for k, v in id_ts.items() if v >= cutoff}
    link_ts = {k: v for k, v in link_ts.items() if v >= cutoff}

    out = {
        "article_ids": list(id_ts.keys()),
        "article_links": list(link_ts.keys()),
        "id_timestamps": id_ts,
        "link_timestamps": link_ts,
        "last_updated": now_iso,
    }
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

def save_selected_articles(articles):
    existing = []
    if Path(SELECTED_FILE).exists():
        try:
            with open(SELECTED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing_links = {a.get("link") for a in existing}
    merged = existing + [a for a in articles if a.get("link") not in existing_links]
    with open(SELECTED_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

def save_stats():
    STATS["timestamp"] = datetime.utcnow().isoformat()
    existing = {}
    if Path(STATS_FILE).exists():
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing.update(STATS)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

# -- UTILITIES -----------------------------------------------------------------

def normalize_link(link, base=None):
    if not link:
        return ""
    link = link.strip()
    if link.startswith("//"):
        link = "https:" + link
    if base and not urlparse(link).netloc:
        link = urljoin(base, link)
    link = re.sub(r"([?&])utm_[^=]+=[^&]+", r"\1", link)
    link = re.sub(r"([?&])fbclid=[^&]+", r"\1", link)
    link = re.sub(r"[?&]$", "", link)
    return link.split("#")[0]

def parse_date(entry):
    for key in ("published_parsed", "updated_parsed", "created_parsed", "issued_parsed"):
        st = entry.get(key)
        if st:
            try:
                dt = datetime.fromtimestamp(time.mktime(st), tz=timezone.utc)
                return dt, False
            except Exception:
                pass
    for key in ("published", "updated", "created", "dc_date", "issued"):
        val = entry.get(key)
        if isinstance(val, str) and val.strip():
            try:
                dt = parsedate_to_datetime(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), False
            except Exception:
                pass
            if dateutil_parser:
                try:
                    dt = dateutil_parser.parse(val)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc), False
                except Exception:
                    pass
    if ALLOW_MISSING_DATES:
        return datetime.now(timezone.utc), True
    return None, False

IMG_SRC_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)

def find_image_in_html(html, base=None):
    if not html:
        return None
    m = IMG_SRC_RE.search(html)
    if not m:
        return None
    return normalize_link(m.group(1).strip(), base=base)

def get_mime_for_url(url):
    if not url:
        return "image/jpeg"
    path = urlparse(url).path.lower()
    if path.endswith(".png"):
        return "image/png"
    if path.endswith(".gif"):
        return "image/gif"
    if path.endswith(".webp"):
        return "image/webp"
    if path.endswith(".svg"):
        return "image/svg+xml"
    return "image/jpeg"

def extract_image_url(entry, base_link=None):
    mt = entry.get("media_thumbnail")
    if mt:
        if isinstance(mt, list) and mt[0].get("url"):
            return normalize_link(mt[0]["url"], base=base_link)
        if isinstance(mt, dict) and mt.get("url"):
            return normalize_link(mt["url"], base=base_link)

    mc = entry.get("media_content")
    if mc:
        if isinstance(mc, list) and mc[0].get("url"):
            return normalize_link(mc[0]["url"], base=base_link)
        if isinstance(mc, dict) and mc.get("url"):
            return normalize_link(mc["url"], base=base_link)

    enc = entry.get("enclosures")
    if enc and isinstance(enc, list):
        for e in enc:
            href = e.get("href") or e.get("url") or e.get("link")
            typ = e.get("type", "")
            if href and (typ.startswith("image/") or re.search(r'\.(jpg|jpeg|png|gif|webp|svg)$', href, re.I)):
                return normalize_link(href, base=base_link)

    links = entry.get("links")
    if links and isinstance(links, list):
        for l in links:
            if l.get("rel") == "enclosure":
                href = l.get("href")
                if href:
                    return normalize_link(href, base=base_link)

    content = entry.get("content")
    if content:
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and c.get("value"):
                    found = find_image_in_html(c.get("value"), base=base_link)
                    if found:
                        return found
        elif isinstance(content, str):
            found = find_image_in_html(content, base=base_link)
            if found:
                return found

    for key in ("summary", "description", "summary_detail", "description_detail"):
        val = entry.get(key)
        if isinstance(val, dict):
            val = val.get("value")
        if isinstance(val, str) and val:
            found = find_image_in_html(val, base=base_link)
            if found:
                return found
    return None

# -- FETCHING ------------------------------------------------------------------

def fetch_via_kl(kl_endpoint, target_feed_url, timeout=20):
    if not kl_endpoint:
        return None
    headers = {"Content-Type": "application/json", "Accept": "application/xml, text/xml, */*"}
    payload = {"url": target_feed_url}
    try:
        resp = requests.post(kl_endpoint, json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    try:
        resp = requests.get(kl_endpoint, params={"url": target_feed_url}, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    return None

def fetch_feed(url):
    url_norm = url.strip()
    method_used = "DIRECT"

    if url_norm in EXISTING_API_FEEDS:
        feed = feedparser.parse(url_norm)
        method_used = "DIRECT"
    elif url_norm in KL_API_FEEDS:
        kl_endpoint = os.environ.get("KL")
        feed = None
        if kl_endpoint:
            feed = fetch_via_kl(kl_endpoint, url_norm)
            if feed:
                method_used = "KL"
        if not feed:
            feed = feedparser.parse(url_norm)
            method_used = "DIRECT"
    else:
        feed = feedparser.parse(url_norm)
        method_used = "DIRECT"

    entries_count = len(getattr(feed, "entries", []))
    STATS["per_feed"].setdefault(url_norm, {"fetched": 0, "passed_age": 0, "capped": 0})
    STATS["per_feed"][url_norm]["fetched"] += entries_count
    STATS["per_method"].setdefault(method_used, 0)
    STATS["per_method"][method_used] += entries_count
    STATS["total_fetched"] += entries_count

    return feed

def fetch_all_feeds():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=MAX_AGE_HOURS)
    bd_now = datetime.now(BD_TZ)
    bd_now_str = bd_now.strftime("%a, %d %b %Y %H:%M:%S +0600")
    all_articles = []

    for url in FEED_URLS:
        feed = fetch_feed(url)
        feed_items = []

        for e in feed.entries:
            dt, inferred = parse_date(e)
            if not dt:
                continue
            if (not ALLOW_OLDER) and dt < cutoff:
                continue

            desc = ""
            if e.get("summary"):
                desc = e.get("summary")
            elif e.get("description"):
                desc = e.get("description")
            elif e.get("content") and isinstance(e.get("content"), list):
                desc = "\n".join([c.get("value", "") for c in e.get("content") if isinstance(c, dict)])
            else:
                det = e.get("summary_detail") or e.get("description_detail")
                if isinstance(det, dict):
                    desc = det.get("value", "") or ""

            link = normalize_link(e.get("link") or "")
            article_id = e.get("id") or link or ""
            image_url = extract_image_url(e, base_link=link)

            article = {
                "id": str(article_id),
                "title": e.get("title", "") or "",
                "link": link,
                "description": desc or "",
                "published": bd_now_str,
                "source": url,
            }
            if inferred:
                article["published_inferred"] = True
            if image_url:
                article["thumbnail"] = image_url
                article["thumbnail_type"] = get_mime_for_url(image_url)

            feed_items.append(article)

        passed = len(feed_items)
        capped = min(passed, MAX_ARTICLES_PER_FEED)
        STATS["per_feed"][url]["passed_age"] = passed
        STATS["per_feed"][url]["capped"] = capped
        STATS["total_passed_age"] += passed
        all_articles.extend(feed_items[:MAX_ARTICLES_PER_FEED])

    return all_articles

def get_new_articles(all_articles, processed_data):
    processed_ids = set(processed_data.get("article_ids", []))
    processed_links = set(processed_data.get("article_links", []))
    new = []
    for a in all_articles:
        aid = a.get("id")
        alink = a.get("link")
        if (aid and aid not in processed_ids) and (alink and alink not in processed_links):
            new.append(a)
        elif alink and alink not in processed_links and aid not in processed_ids:
            new.append(a)
    return new

# -- CLASSIFICATION ------------------------------------------------------------

def extract_json_object(text):
    text = text.replace("```json", "").replace("```", "").strip()
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return {"signal": [i for i in obj.get("signal", []) if isinstance(i, int)]}
        except Exception:
            pass
    result = {"signal": []}
    m = re.search(r'"signal"\s*:\s*(\[.*?\])', text, flags=re.DOTALL)
    if m:
        try:
            result["signal"] = [i for i in json.loads(m.group(1)) if isinstance(i, int)]
        except Exception:
            pass
    return result

def send_to_mistral(articles):
    api_key = os.environ.get("MS")
    if not api_key or not articles:
        return []

    try:
        client = Mistral(api_key=api_key)
        titles_text = "\n".join([f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)])

        response = client.chat.complete(
            model=MISTRAL_MODEL,
            messages=[{"role": "user", "content": BANGLA_PROMPT.format(titles=titles_text)}],
            response_format={"type": "json_object"},
        )

        text = response.choices[0].message.content or ""
        return extract_json_object(text).get("signal", [])

    except Exception as e:
        print(f"Mistral classification error: {e}")
        return []

def _normalize_title_for_dedup(title: str) -> str:
    title = (title or "").strip().lower()
    title = re.sub(r"[\u200c\u200d]", "", title)
    title = re.sub(r"[\W_]+", " ", title, flags=re.UNICODE)
    title = re.sub(r"\s+", " ", title).strip()
    return title

def _title_similarity(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()

def deduplicate_articles(articles):
    if not articles:
        return articles

    kept = []
    normalized_seen = []

    for article in articles:
        title = article.get("title", "") or ""
        norm_title = _normalize_title_for_dedup(title)
        is_duplicate = False

        for prev in normalized_seen:
            if norm_title == prev:
                is_duplicate = True
                break
            if norm_title and prev and _title_similarity(norm_title, prev) >= 0.88:
                is_duplicate = True
                break
            if norm_title in prev or prev in norm_title:
                is_duplicate = True
                break

        if not is_duplicate:
            kept.append(article)
            normalized_seen.append(norm_title)

    dropped = len(articles) - len(kept)
    if dropped:
        print(f"Dedup: removed {dropped} near-duplicate title(s).")
    return kept

# -- XML -----------------------------------------------------------------------

def _fresh_channel(root, feed_title, feed_description):
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text = feed_title
    ET.SubElement(channel, "link").text = "https://yourusername.github.io/yourrepo/"
    ET.SubElement(channel, "description").text = feed_description
    return channel

def _load_or_create(output_file, feed_title, feed_description):
    ET.register_namespace("media", MEDIA_NS)
    if Path(output_file).exists():
        try:
            tree = ET.parse(output_file)
            root = tree.getroot()
            channel = root.find("channel")
            if channel is not None:
                return tree, root, channel
            channel = _fresh_channel(root, feed_title, feed_description)
            return tree, root, channel
        except ET.ParseError:
            pass
    root = ET.Element("rss", {"version": "2.0"})
    tree = ET.ElementTree(root)
    channel = _fresh_channel(root, feed_title, feed_description)
    return tree, root, channel

def generate_xml_feed(articles, output_file, feed_title=None, feed_description=None):
    feed_title = feed_title or "Curated News"
    feed_description = feed_description or "AI-curated news feed"

    tree, root, channel = _load_or_create(output_file, feed_title, feed_description)

    existing_links: set[str] = set()
    for item in channel.findall("item"):
        link_el = item.find("link")
        if link_el is not None and link_el.text:
            existing_links.add(link_el.text.strip())

    added = 0
    for a in articles:
        link = (a.get("link") or "").strip()
        if not link or link in existing_links:
            continue

        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = a.get("title", "") or ""
        ET.SubElement(item, "link").text = link
        guid_val = a.get("id") or link
        is_permalink = "true" if guid_val.startswith("http") else "false"
        ET.SubElement(item, "guid", {"isPermaLink": is_permalink}).text = guid_val
        ET.SubElement(item, "description").text = a.get("description", "") or ""
        if a.get("published"):
            ET.SubElement(item, "pubDate").text = a["published"]

        thumb = a.get("thumbnail")
        if thumb:
            ET.SubElement(item, MEDIA_TAG + "thumbnail", {"url": thumb})
            mime = a.get("thumbnail_type") or get_mime_for_url(thumb)
            ET.SubElement(item, "enclosure", {"url": thumb, "type": mime, "length": "0"})

        existing_links.add(link)
        added += 1

    all_items = channel.findall("item")
    overflow = len(all_items) - MAX_FEED_ITEMS
    if overflow > 0:
        for old_item in all_items[:overflow]:
            channel.remove(old_item)

    now_text = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
    last_build = channel.find("lastBuildDate")
    if last_build is None:
        ET.SubElement(channel, "lastBuildDate").text = now_text
    else:
        last_build.text = now_text

    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    tree.write(output_file, encoding="unicode", xml_declaration=False)

    with open(output_file, "r+", encoding="utf-8") as fh:
        body = fh.read()
        fh.seek(0)
        fh.write('<?xml version="1.0" encoding="UTF-8"?>\n' + body)
        fh.truncate()

    return added

# -- STATS ---------------------------------------------------------------------

def print_stats():
    print("\nFetch statistics:")
    print(f"  Timestamp:               {STATS.get('timestamp')}")
    print(f"  Total fetched:           {STATS['total_fetched']}  (raw entries from all feeds)")
    print(f"  Passed age cut:          {STATS['total_passed_age']}  (within {MAX_AGE_HOURS}h window)")
    print(f"  New (unseen):            {STATS['total_new']}")
    print(f"    ├─ Bangla (classified): {STATS['total_bangla']}")
    print(f"    └─ Non-Bangla (skipped): {STATS['total_skipped_non_bangla']}")
    print(f"  Signal (Mistral):        {STATS['total_signal_mistral']}")
    print(f"  Signal (after dedup):    {STATS['total_signal_deduped']}  -> {OUTPUT_XML}")
    print("  Per-method (raw fetch):")
    for method, cnt in STATS["per_method"].items():
        print(f"    {method}: {cnt}")
    print("  Per-feed breakdown:")
    for feed, d in STATS["per_feed"].items():
        print(f"    {feed}")
        print(f"      fetched={d.get('fetched',0)}  passed_age={d.get('passed_age',0)}  sent_to_pipeline={d.get('capped',0)}")
    print("")

# -- MAIN ----------------------------------------------------------------------

def main():
    processed_data = load_processed_articles()
    all_articles = fetch_all_feeds()
    new_articles = get_new_articles(all_articles, processed_data)

    STATS["total_new"] = len(new_articles)

    bangla_articles = [a for a in new_articles if is_bangla_title(a.get("title", ""))]
    non_bangla_count = len(new_articles) - len(bangla_articles)

    STATS["total_bangla"] = len(bangla_articles)
    STATS["total_skipped_non_bangla"] = non_bangla_count

    print(f"New articles: {len(new_articles)} total  |  {len(bangla_articles)} Bangla (classifying)  |  {non_bangla_count} non-Bangla (skipped)")

    if not bangla_articles:
        print("No new Bangla articles to classify.")
        print_stats()
        return

    print(f"Classifying {len(bangla_articles)} Bangla article(s)...")

    mistral_indices = send_to_mistral(bangla_articles)
    mistral_indices = [i for i in mistral_indices if 0 <= i < len(bangla_articles)]

    STATS["total_signal_mistral"] = len(mistral_indices)
    STATS["total_signal"] = len(mistral_indices)

    print(f"  → Mistral: {len(mistral_indices)}")

    if not mistral_indices:
        print("Mistral returned 0 signal. Skipping all file writes.")
        print_stats()
        return

    signal_articles = [bangla_articles[i] for i in mistral_indices]

    print(f"Deduplicating {len(signal_articles)} signal article(s)...")
    signal_articles = deduplicate_articles(signal_articles)

    STATS["total_signal_deduped"] = len(signal_articles)

    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="Curated Bangla Editorials",
        feed_description="AI-curated signal: Bangla editorials on Bangladesh and world affairs",
    )

    save_selected_articles(signal_articles)

    processed_data.setdefault("article_ids", []).extend([a["id"] for a in new_articles if a.get("id")])
    processed_data.setdefault("article_links", []).extend([a["link"] for a in new_articles if a.get("link")])
    save_processed_articles(processed_data)

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()
    print_stats()

if __name__ == "__main__":
    main()
