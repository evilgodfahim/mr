#!/usr/bin/env python3
"""
RSS Feed Processor with Gemini API Integration (robust date/content handling + thumbnails)

All articles from all feeds go through two parallel Gemini classification paths:
  - Non-Bangla titles  → international/BD hard-news prompt
  - Bangla-script titles → Bangla editorial prompt

Both signal buckets are merged, then deduplicated in a single Gemini call.

Outputs:
  curated_feed.xml  - signal articles (both streams)
Stats:
  fetch_stats.json
"""

import feedparser
import json
import os
import time
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from google import genai
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse

import requests

try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# -- FEEDS ---------------------------------------------------------------------

FEED_URLS = [
    "https://evilgodfahim.github.io/gpd/daily_feed.xml",
    "https://evilgodfahim.github.io/edit/daily_feed.xml",
    "https://evilgodfahim.github.io/bdl/final.xml",
    "https://evilgodfahim.github.io/daily/daily_master.xml",
    "https://evilgodfahim.github.io/int/final.xml",
    "https://evilgodfahim.github.io/fp/final.xml",
    "https://evilgodfahim.github.io/org/daily_feed.xml",
    "https://evilgodfahim.github.io/bangladesh/feed.xml",
]

EXISTING_API_FEEDS = {
    "https://evilgodfahim.github.io/gpd/daily_feed.xml",
    "https://evilgodfahim.github.io/edit/daily_feed.xml",
    "https://evilgodfahim.github.io/bdl/final.xml",
    "https://evilgodfahim.github.io/daily/daily_master.xml",
    "https://evilgodfahim.github.io/int/final.xml",
    "https://evilgodfahim.github.io/fp/final.xml",
    "https://evilgodfahim.github.io/org/daily_feed.xml",
    "https://evilgodfahim.github.io/bangladesh/feed.xml",
}

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

GEMINI_MODEL          = "gemini-3-flash-preview"
DEDUP_MODEL           = "gemini-2.5-flash"
PROCESSED_FILE        = "processed_articles.json"
SELECTED_FILE         = "selected_articles.json"
OUTPUT_XML            = "curated_feed.xml"
STATS_FILE            = "fetch_stats.json"
MAX_ARTICLES_PER_FEED = 100
MAX_AGE_HOURS         = 26
ALLOW_MISSING_DATES   = True
ALLOW_OLDER           = False
MAX_FEED_ITEMS        = 500

# -- BANGLA FILTER -------------------------------------------------------------

# Unicode block for Bengali script: U+0980 – U+09FF
BANGLA_RE = re.compile(r"[\u0980-\u09FF]")


def is_bangla_title(title: str) -> bool:
    """Return True only if the title contains meaningful Bengali script (≥4 chars)."""
    if not title:
        return False
    return sum(1 for c in title if "\u0980" <= c <= "\u09FF") >= 4

# -- PROMPTS -------------------------------------------------------------------

# Prompt for non-Bangla (international + English BD) titles
PROMPT = """You are a strict news classification engine. Input: numbered article titles from news outlets, geopolitical journals, and Bangladeshi newspapers — including hard news, editorials, op-eds, and essays. Classify each as SIGNAL or NOISE. Return only SIGNAL indices. The bar is HIGH.

STEP 1 — INSTANT NOISE. Stop here if the title is any of:
  Sports · entertainment · celebrity · lifestyle · human interest · tribute or commemorative · praise of a person, party, or institution · isolated local incident (one district, one institution, one community)

STEP 2 — IS BANGLADESH DIRECTLY INVOLVED?

  YES → SIGNAL if:
  a) National scale: affects the whole country or a significant portion of the population. Cause is irrelevant — government decision, economic condition, failing public system, environmental crisis, infrastructure breakdown, natural disaster, social emergency, health situation. If the reach is national, it is SIGNAL.
  b) Foreign affairs: any substantive BD external development — bilateral talks or disputes, international pressure or sanctions on BD, foreign aid or loans, cross-border issues (water, trade, security, migration), BD at international forums, international bodies acting on BD. If BD is a direct party, it is SIGNAL. Do not mistake substantive diplomacy for routine ceremony.
  c) Editorial naming a concrete national-scale domain or condition → SIGNAL. Vague sentiment with no named domain → NOISE. Party strategy or partisan praise → NOISE.

  NO → SIGNAL if:
  a) Multinational bodies acting collectively: UN and agencies, NATO, IMF, World Bank, WTO, G7/G20, BRICS, IAEA, ICC, ICJ, regional alliances. Their resolutions, findings, and interventions are SIGNAL by nature.
  b) Multi-country events: wars, conflicts, cross-border crises, multilateral treaties, regional instability, international sanctions.
  c) Single-country decision with cross-border consequence — two types:
     Immediate: moves something the world depends on (global energy supply, global financial systems, pandemic-level health, global trade architecture).
     Strategic/slow-burn: shifts power, security, or stability even without immediate surface effect — nuclear decisions, major arms deals or military build-up, upstream water control affecting downstream countries, military base shifts, significant cyber operations, treaty withdrawals. Ask: does this change what is possible or what is threatened in the world?
  All other single-country internal affairs → NOISE.

WHEN IN DOUBT → NOISE.

Output only: {{"signal": [0-based indices]}}. Valid JSON, no markdown, no explanation.

EXAMPLES:

Input:
0. US and China sign landmark trade agreement
1. Premier League club sacks manager
2. Bangladesh central bank raises interest rates amid inflation crisis
3. UK Conservative Party elects new leader
4. UN warns of imminent famine across the Horn of Africa
5. The Promise of a New Bangladesh
6. We Must Fix Bangladesh's Broken Irrigation System
7. Saluting the Spirit of Our Freedom Fighters
8. Bangladesh slashes fuel subsidies nationwide
9. India's internal border dispute heats up
10. Bangladesh foreign minister holds talks with India over Teesta water sharing
11. US warns Bangladesh over labour rights ahead of trade review
12. China pledges $3bn infrastructure investment in Bangladesh
13. NATO expands eastern flank military presence
14. India builds new dam on Brahmaputra upstream of Bangladesh
Output: {{"signal": [0, 2, 4, 6, 8, 10, 11, 12, 13, 14]}}

Input:
0. India and Pakistan exchange fire across Line of Control
1. Dhaka garment workers strike shuts down hundreds of factories
2. Australia holds federal election
3. IMF approves emergency loan for Bangladesh
4. BNP's Path Forward After the Election
5. How Microfinance Is Changing Lives in Sylhet
6. How Poor Water Management Is Destroying Bangladesh's Agriculture
7. The Geopolitics of the Indo-Pacific and What It Means for the World
8. Why [Party Leader] Is the Leader Bangladesh Deserves
9. IAEA raises alarm over Iran's uranium enrichment levels
10. The Slow Collapse of Bangladesh's River Systems
11. Why Bangladesh's Public Hospitals Are Failing the Poor
Output: {{"signal": [0, 1, 3, 6, 7, 9, 10, 11]}}

Article titles:
{titles}
"""

# Prompt for Bangla-script editorial/op-ed titles
BANGLA_PROMPT = """You are a strict editorial classifier for Bangladeshi Bengali-language opinion journalism.
Input: numbered Bengali editorial and op-ed titles from Bangladeshi newspapers.
Classify each as SIGNAL or NOISE. Return only SIGNAL indices. The bar is HIGH.

CORE QUESTION: Does this title name a concrete, substantive domain of national public concern?

STEP 1 — INSTANT NOISE. Stop here if the title is any of:
  Tribute or memorial · praise of a leader, party, or institution · commemorative piece · sports or entertainment · lifestyle or human-interest · single-district or single-institution local issue with no national implication · personal or religious inspiration with no policy dimension · cultural nostalgia

STEP 2 — SIGNAL if the title:
  a) Names a concrete national-scale domain or condition — economy, inflation, banking, governance failure, public health system, environmental crisis, river erosion, flood, infrastructure breakdown, education quality, labour rights, energy, food security, law and order, judicial system, press freedom, constitutional matters. Domain must be explicitly inferable from the title.
  b) Critiques or analyses a specific policy, institution, or systemic condition at national scale — not vague exhortation.
  c) Addresses a Bangladesh foreign-affairs dimension from an editorial/analytical angle — water rights (Teesta, Brahmaputra), bilateral disputes, trade, migration, cross-border issues.
  d) Engages with a global or regional issue that has direct consequence for Bangladesh — climate, global food prices, regional security — even if Bangladesh is not named in the title.

STEP 3 — NOISE if:
  Aspirational or exhortational with no named domain · partisan praise or attack · vague moral commentary · personal biography

WHEN IN DOUBT → NOISE.

Output only: {{"signal": [0-based indices]}}. Valid JSON, no markdown, no explanation.

EXAMPLES:

Input:
0. বাংলাদেশের বিদ্যুৎ সংকট কি স্থায়ী সমাধানের পথে
1. আমাদের এগিয়ে যেতে হবে
2. তিস্তার পানি ও বাংলাদেশের কৃষির ভবিষ্যৎ
3. এক মহান নেতার প্রতি শ্রদ্ধাঞ্জলি
4. সরকারি হাসপাতালে দুর্নীতি ও জনভোগান্তি
5. স্বপ্নের বাংলাদেশ গড়ার প্রতিশ্রুতি
6. মূল্যস্ফীতির চাপে সাধারণ মানুষের জীবন
7. নদী দখল ও পরিবেশ বিপর্যয়ের দায় কার
8. দলীয় আদর্শই আমাদের পথ দেখাবে
9. শিক্ষাব্যবস্থার সংকট ও করণীয়
Output: {{"signal": [0, 2, 4, 6, 7, 9]}}

Input:
0. গণমাধ্যমের স্বাধীনতা ও রাষ্ট্রের দায়িত্ব
1. বিজয় দিবসের চেতনায় আলোকিত হোক প্রজন্ম
2. ব্যাংক খাতের খেলাপি ঋণ এবং আর্থিক স্থিতিশীলতা
3. নেতার জন্মদিনে আমাদের অঙ্গীকার
4. জলবায়ু পরিবর্তন ও বাংলাদেশের উপকূলীয় সংকট
5. বাজারে সিন্ডিকেটের দৌরাত্ম্য কতদিন চলবে
6. ধর্মীয় সম্প্রীতির অনুপ্রেরণায় এগিয়ে চলি
7. স্বাস্থ্যসেবায় বৈষম্য ও রাষ্ট্রের ব্যর্থতা
Output: {{"signal": [0, 2, 4, 5, 7]}}

Article titles:
{titles}
"""

DEDUP_PROMPT = """You are a news deduplication engine. You will receive a numbered list of article titles.
Your task: identify groups of titles that cover the same story or event (near-duplicates, rephrased versions, or very similar headlines). For each such group, keep only the FIRST occurrence (lowest index) and discard the rest.
Titles that cover clearly distinct topics must all be kept.

Rules:
- Return only the indices (0-based) of titles to KEEP, as a JSON array of integers.
- Always keep at least one title from each duplicate group (the one with the lowest index).
- If all titles are unique, return all indices.
- Return only valid JSON. No markdown, no backticks, no preamble. Example output: [0, 1, 3, 5]

Article titles:
{titles}
"""

# -- CONSTANTS -----------------------------------------------------------------

MEDIA_NS  = "http://search.yahoo.com/mrss/"
MEDIA_TAG = "{%s}" % MEDIA_NS
ET.register_namespace("media", MEDIA_NS)

BD_TZ = timezone(timedelta(hours=6))

STATS = {
    "per_feed":              {},
    "per_method":            {"KL": 0, "DIRECT": 0},
    "total_fetched":         0,
    "total_passed_age":      0,
    "total_new":             0,
    "total_new_bangla":      0,
    "total_new_other":       0,
    "total_signal":          0,
    "total_signal_deduped":  0,
    "timestamp":             None,
}

# -- I/O -----------------------------------------------------------------------

def load_processed_articles():
    if Path(PROCESSED_FILE).exists():
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "article_ids":   data.get("article_ids", []),
                "article_links": data.get("article_links", []),
                "last_updated":  data.get("last_updated"),
            }
        except Exception:
            pass
    return {"article_ids": [], "article_links": [], "last_updated": None}


def save_processed_articles(data):
    data["article_ids"]   = list(dict.fromkeys(data.get("article_ids", [])))
    data["article_links"] = list(dict.fromkeys(data.get("article_links", [])))
    data["last_updated"]  = datetime.utcnow().isoformat()
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


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
    link = re.sub(r"([?&])fbclid=[^&]+",    r"\1", link)
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
    if path.endswith(".png"):  return "image/png"
    if path.endswith(".gif"):  return "image/gif"
    if path.endswith(".webp"): return "image/webp"
    if path.endswith(".svg"):  return "image/svg+xml"
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
            typ  = e.get("type", "")
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
    url_norm    = url.strip()
    method_used = "DIRECT"

    if url_norm in EXISTING_API_FEEDS:
        feed        = feedparser.parse(url_norm)
        method_used = "DIRECT"
    elif url_norm in KL_API_FEEDS:
        kl_endpoint = os.environ.get("KL")
        feed        = None
        if kl_endpoint:
            feed = fetch_via_kl(kl_endpoint, url_norm)
            if feed:
                method_used = "KL"
        if not feed:
            feed        = feedparser.parse(url_norm)
            method_used = "DIRECT"
    else:
        feed        = feedparser.parse(url_norm)
        method_used = "DIRECT"

    entries_count = len(getattr(feed, "entries", []))
    STATS["per_feed"].setdefault(url_norm, {"fetched": 0, "passed_age": 0, "capped": 0})
    STATS["per_feed"][url_norm]["fetched"] += entries_count
    STATS["per_method"].setdefault(method_used, 0)
    STATS["per_method"][method_used] += entries_count
    STATS["total_fetched"]            += entries_count

    return feed


def fetch_all_feeds():
    now        = datetime.now(timezone.utc)
    cutoff     = now - timedelta(hours=MAX_AGE_HOURS)
    bd_now     = datetime.now(BD_TZ)
    bd_now_str = bd_now.strftime("%a, %d %b %Y %H:%M:%S +0600")
    all_articles = []

    for url in FEED_URLS:
        feed       = fetch_feed(url)
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

            link       = normalize_link(e.get("link") or "")
            article_id = e.get("id") or link or ""
            image_url  = extract_image_url(e, base_link=link)

            article = {
                "id":          str(article_id),
                "title":       e.get("title", "") or "",
                "link":        link,
                "description": desc or "",
                "published":   bd_now_str,
                "source":      url,
            }
            if inferred:
                article["published_inferred"] = True
            if image_url:
                article["thumbnail"]      = image_url
                article["thumbnail_type"] = get_mime_for_url(image_url)

            feed_items.append(article)

        passed = len(feed_items)
        capped = min(passed, MAX_ARTICLES_PER_FEED)
        STATS["per_feed"][url]["passed_age"] = passed
        STATS["per_feed"][url]["capped"]     = capped
        STATS["total_passed_age"]           += passed
        all_articles.extend(feed_items[:MAX_ARTICLES_PER_FEED])

    return all_articles


def get_new_articles(all_articles, processed_data):
    processed_ids   = set(processed_data.get("article_ids", []))
    processed_links = set(processed_data.get("article_links", []))
    new = []
    for a in all_articles:
        aid   = a.get("id")
        alink = a.get("link")
        if (aid and aid not in processed_ids) and (alink and alink not in processed_links):
            new.append(a)
        elif alink and alink not in processed_links and aid not in processed_ids:
            new.append(a)
    return new

# -- GEMINI --------------------------------------------------------------------

def extract_json_object(text):
    """Parse {"signal": [...]} from Gemini response."""
    text = text.replace("```json", "").replace("```", "").strip()
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return {
                    "signal": [i for i in obj.get("signal", []) if isinstance(i, int)],
                }
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


def send_to_gemini(articles, prompt_template):
    """
    Single Gemini classification call.
    prompt_template must contain a {titles} placeholder OR be the full system prompt
    that expects titles appended as user content.
    Returns {"signal": [local 0-based indices]}.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key or not articles:
        return {"signal": []}

    try:
        client = genai.Client(api_key=api_key)

        titles_text = "\n".join(
            [f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)]
        )

        # PROMPT uses {titles} inline; BANGLA_PROMPT also uses {titles}
        full_prompt = prompt_template.format(titles=titles_text)

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=full_prompt,
            config={"response_mime_type": "application/json"},
        )

        if hasattr(response, "parsed") and response.parsed:
            return {
                "signal": [i for i in response.parsed.get("signal", []) if isinstance(i, int)],
            }

        return extract_json_object(response.text)

    except Exception as e:
        print(f"Gemini classification error: {e}")
        return {"signal": []}


def deduplicate_articles(articles):
    """
    Send article titles to Gemini 2.5 Flash.
    Returns a deduplicated subset of `articles`, preserving order.
    Falls back to returning all articles unchanged on any error.
    """
    if not articles:
        return articles

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return articles

    try:
        client = genai.Client(api_key=api_key)

        titles_text = "\n".join(
            [f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)]
        )

        response = client.models.generate_content(
            model=DEDUP_MODEL,
            contents=DEDUP_PROMPT.format(titles=titles_text),
            config={"response_mime_type": "application/json"},
        )

        raw = response.text if hasattr(response, "text") else ""
        raw = raw.replace("```json", "").replace("```", "").strip()

        keep_indices = None
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                keep_indices = [i for i in parsed if isinstance(i, int) and 0 <= i < len(articles)]
        except Exception:
            pass

        if keep_indices is None:
            m = re.search(r"\[[\d,\s]+\]", raw)
            if m:
                try:
                    keep_indices = [
                        i for i in json.loads(m.group(0))
                        if isinstance(i, int) and 0 <= i < len(articles)
                    ]
                except Exception:
                    pass

        if keep_indices is None:
            print("Dedup: could not parse response, keeping all articles.")
            return articles

        keep_indices = sorted(set(keep_indices))
        deduped = [articles[i] for i in keep_indices]
        dropped = len(articles) - len(deduped)
        if dropped:
            print(f"Dedup: removed {dropped} near-duplicate title(s).")
        return deduped

    except Exception as e:
        print(f"Gemini dedup error: {e}")
        return articles

# -- XML -----------------------------------------------------------------------

def _fresh_channel(root, feed_title, feed_description):
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text       = feed_title
    ET.SubElement(channel, "link").text        = "https://yourusername.github.io/yourrepo/"
    ET.SubElement(channel, "description").text = feed_description
    return channel


def _load_or_create(output_file, feed_title, feed_description):
    ET.register_namespace("media", MEDIA_NS)

    if Path(output_file).exists():
        try:
            tree    = ET.parse(output_file)
            root    = tree.getroot()
            channel = root.find("channel")
            if channel is not None:
                return tree, root, channel
            channel = _fresh_channel(root, feed_title, feed_description)
            return tree, root, channel
        except ET.ParseError:
            pass

    root    = ET.Element("rss", {"version": "2.0"})
    tree    = ET.ElementTree(root)
    channel = _fresh_channel(root, feed_title, feed_description)
    return tree, root, channel


def generate_xml_feed(articles, output_file, feed_title=None, feed_description=None):
    feed_title       = feed_title       or "Curated News"
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
        ET.SubElement(item, "title").text       = a.get("title", "") or ""
        ET.SubElement(item, "link").text        = link
        guid_val     = a.get("id") or link
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
    overflow  = len(all_items) - MAX_FEED_ITEMS
    if overflow > 0:
        for old_item in all_items[:overflow]:
            channel.remove(old_item)

    now_text   = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
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
    print(f"  Timestamp:            {STATS.get('timestamp')}")
    print(f"  Total fetched:        {STATS['total_fetched']}  (raw entries from all feeds)")
    print(f"  Passed age cut:       {STATS['total_passed_age']}  (within {MAX_AGE_HOURS}h window)")
    print(f"  New (unseen):         {STATS['total_new']}")
    print(f"    ├─ Bangla titles:   {STATS['total_new_bangla']}")
    print(f"    └─ Other titles:    {STATS['total_new_other']}")
    print(f"  Signal (classified):  {STATS['total_signal']}")
    print(f"  Signal (after dedup): {STATS['total_signal_deduped']}  -> {OUTPUT_XML}")
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
    all_articles   = fetch_all_feeds()
    new_articles   = get_new_articles(all_articles, processed_data)

    STATS["total_new"] = len(new_articles)

    # --- Split into Bangla vs non-Bangla --------------------------------------
    bangla_articles = [a for a in new_articles if is_bangla_title(a.get("title", ""))]
    other_articles  = [a for a in new_articles if not is_bangla_title(a.get("title", ""))]

    STATS["total_new_bangla"] = len(bangla_articles)
    STATS["total_new_other"]  = len(other_articles)

    print(f"New articles: {len(new_articles)} total  |  {len(bangla_articles)} Bangla  |  {len(other_articles)} other")

    # --- Step 1a: classify non-Bangla articles --------------------------------
    signal_articles = []

    if other_articles:
        print(f"Classifying {len(other_articles)} non-Bangla article(s)...")
        result_other   = send_to_gemini(other_articles, PROMPT)
        signal_other   = [
            other_articles[i]
            for i in result_other.get("signal", [])
            if isinstance(i, int) and 0 <= i < len(other_articles)
        ]
        print(f"  → {len(signal_other)} signal")
        signal_articles.extend(signal_other)

    # --- Step 1b: classify Bangla editorial titles ----------------------------
    if bangla_articles:
        print(f"Classifying {len(bangla_articles)} Bangla article(s)...")
        result_bangla  = send_to_gemini(bangla_articles, BANGLA_PROMPT)
        signal_bangla  = [
            bangla_articles[i]
            for i in result_bangla.get("signal", [])
            if isinstance(i, int) and 0 <= i < len(bangla_articles)
        ]
        print(f"  → {len(signal_bangla)} signal")
        signal_articles.extend(signal_bangla)

    STATS["total_signal"] = len(signal_articles)

    # --- Step 2: deduplicate merged signal set --------------------------------
    print(f"Deduplicating {len(signal_articles)} signal article(s)...")
    signal_articles = deduplicate_articles(signal_articles)

    STATS["total_signal_deduped"] = len(signal_articles)

    # --- Step 3: write to the shared XML feed ---------------------------------
    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="Curated News",
        feed_description="AI-curated signal: international affairs, Bangladesh news, and Bangla editorials",
    )

    save_selected_articles(signal_articles)

    processed_data.setdefault("article_ids",   []).extend([a["id"]   for a in new_articles if a.get("id")])
    processed_data.setdefault("article_links", []).extend([a["link"] for a in new_articles if a.get("link")])
    save_processed_articles(processed_data)

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()
    print_stats()


if __name__ == "__main__":
    main()
