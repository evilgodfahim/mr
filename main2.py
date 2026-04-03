#!/usr/bin/env python3
"""
RSS Feed Processor

All articles from all feeds go to one Mistral call.
Mistral classifies each headline into signal or noise.
A separate deduplication step removes near-duplicate signal titles.

Outputs:
  curated_feed.xml  - signal articles
  ex.xml            - excluded articles
Stats:
  fetch_stats_main.json
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
    "https://evilgodfahim.github.io/bdlb/final.xml",
    "https://evilgodfahim.github.io/bint/final.xml",
    "https://www.kalerkantho.com/rss.xml",
    "https://www.prothomalo.com/feed/",
    "https://evilgodfahim.github.io/ju/rss.xml",
    "https://evilgodfahim.github.io/dnpbb/feed.xml",
    "https://evilgodfahim.github.io/jnp/editorial_news.xml",
    "https://evilgodfahim.github.io/kk/daily_kalerkantho_part2.xml",
    "https://evilgodfahim.github.io/kk/daily_kalerkantho_part1.xml",
    "https://evilgodfahim.github.io/kal/articles.xml",
    "https://evilgodfahim.github.io/bb-rss/feed.xml",

"https://evilgodfahim.github.io/pade/output/articles.xml",

"https://evilgodfahim.github.io/pade/output/articles_2.xml"
]

EXISTING_API_FEEDS = {
    "https://evilgodfahim.github.io/bdlb/final.xml",
    "https://evilgodfahim.github.io/bint/final.xml",
    "https://www.kalerkantho.com/rss.xml",
    "https://www.prothomalo.com/feed/",
    "https://evilgodfahim.github.io/ju/rss.xml",
    "https://evilgodfahim.github.io/dnpbb/feed.xml",
    "https://evilgodfahim.github.io/jnp/editorial_news.xml",
    "https://evilgodfahim.github.io/kk/daily_kalerkantho_part2.xml",
    "https://evilgodfahim.github.io/kk/daily_kalerkantho_part1.xml",
    "https://evilgodfahim.github.io/kal/articles.xml",
    "https://evilgodfahim.github.io/bb-rss/feed.xml",

"https://evilgodfahim.github.io/pade/output/articles.xml",

"https://evilgodfahim.github.io/pade/output/articles_2.xml"
}

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

MISTRAL_MODEL         = "mistral-large-latest"
PROCESSED_FILE        = "processed_articles_mainb.json"
SELECTED_FILE         = "selected_articles_mainb.json"
OUTPUT_XML            = "curated_feedb.xml"
EXCLUDED_XML          = "exb.xml"
STATS_FILE            = "fetch_stats_mainb.json"
MAX_ARTICLES_PER_FEED = 100
MAX_AGE_HOURS         = 26
ALLOW_MISSING_DATES   = True
ALLOW_OLDER           = False
MAX_FEED_ITEMS        = 500
RETENTION_DAYS        = 10

# -- PROMPTS -------------------------------------------------------------------

PROMPT = """You are a strict news classification engine. Input: numbered article titles from news outlets and Bangladeshi newspapers. Classify each as SIGNAL or NOISE. Return only SIGNAL indices. Only English language titles will be considered. The bar is SUPER HIGH; (LOWEST < LOWER < LOW < AVERAGE < HIGH < SUPER HIGH < ULTRA HIGH < EXTREME).

STEP 1 — INSTANT NOISE. Mark as NOISE immediately if the title is any of:
  - Sports, entertainment, celebrity, lifestyle, human interest
  - Tribute, commemorative, or anniversary pieces
  - Praise or criticism of a person, party, or institution
  - Any isolated or discrete incident: one arrest, one clash, one crime, one accident, one fire, one death, one protest at one location — no matter how dramatic the title sounds
  - Anything affecting only one district, one institution, one community, or one individual

STEP 2 — SCOPE CHECK.

  BANGLADESH: SIGNAL only if the event or decision affects the entire country or a nationally significant portion of it:
  - Economic data or official decisions: central bank actions, national budget, trade figures, remittance data, fuel/utility price changes, foreign reserve status, currency moves, stock market circuit breakers, IMF/World Bank actions on BD
  - Government or institutional actions at the national level: cabinet decisions, parliament acts, nationwide policy rollouts, supreme court rulings, election commission decisions
  - Infrastructure or public systems at national scale: nationwide power outages, countrywide internet disruption, collapse of a national system (not one hospital, one road, one factory)
  - Natural disasters or health emergencies declared at national or divisional scale (not one district)
  - Foreign affairs: official bilateral talks, international sanctions or pressure on BD, cross-border agreements or disputes (Teesta, Rohingya, trade), BD at UN/IMF/WTO, foreign loans or aid formally approved
  - Anything sub-national, sub-institutional, or about a single individual → NOISE

  INTERNATIONAL: SIGNAL only for concrete events with verified cross-border consequences:
  - Active armed conflicts between states, or formal declarations of war or ceasefire
  - Multinational body decisions: UN Security Council resolutions, IMF/World Bank program approvals, WTO rulings, NATO formal decisions, IAEA findings, ICC/ICJ verdicts
  - Formal multilateral treaties signed or collapsed
  - A single country's decision only if it moves something the world depends on immediately: global energy supply disruption, collapse of a major financial system, verified nuclear weapons development milestone, formal treaty withdrawal with immediate effect
  - Internal politics, elections, leadership changes, and domestic policy of any single foreign country → NOISE unless the direct cross-border consequence is stated in the title itself

WHEN IN DOUBT → NOISE.

Output only: {{"signal": [0-based indices]}}. Valid JSON, no markdown, no explanation.

EXAMPLES:

Input:
0. US and China sign landmark trade agreement
1. Premier League club sacks manager
2. Bangladesh central bank raises interest rates amid inflation crisis
3. UK Conservative Party elects new leader
4. UN Security Council votes to deploy peacekeepers to Sudan
5. The Promise of a New Bangladesh
6. We Must Fix Bangladesh's Broken Irrigation System
7. Bangladesh slashes fuel subsidies nationwide
8. India arrests opposition leader
9. Bangladesh foreign minister holds talks with India over Teesta water sharing
10. US warns Bangladesh over labour rights ahead of GSP review
11. China pledges $3bn infrastructure loan to Bangladesh, deal signed
12. NATO formally approves expansion of eastern flank forces
13. Student clash reported in Dhaka university campus
14. Why Bangladesh's Economy Is at a Crossroads
Output: {{"signal": [0, 2, 4, 7, 9, 10, 11, 12]}}

Input:
0. Pakistan and India exchange fire across Line of Control, casualties confirmed
1. Dhaka garment workers strike shuts down hundreds of factories nationwide
2. Australia holds federal election
3. IMF formally approves $4.7bn loan for Bangladesh
4. BNP's Path Forward After the Election
5. How Microfinance Is Changing Lives in Sylhet
6. The Geopolitics of the Indo-Pacific and What It Means for the World
7. IAEA confirms Iran has enriched uranium to 84 percent purity
8. Man arrested in Chattogram over murder
9. Bangladesh foreign reserves fall below $20bn, taka hits record low
10. Garment exports decline 12% in Q1, Bangladesh Bank reports
11. ICC issues arrest warrant for sitting head of state
12. Fire breaks out at Tejgaon factory, 3 killed
13. Bangladesh parliament passes new cybersecurity law
Output: {{"signal": [0, 1, 3, 7, 9, 10, 11, 13]}}

Article titles:
{titles}
"""

BANGLA_PROMPT = """You are a strict news classification engine. Input: numbered article titles in Bengali script from Bangladeshi newspapers. Classify each as SIGNAL or NOISE. Return only SIGNAL indices. Only Bengali language titles will be considered. The bar is SUPER HIGH; (LOWEST < LOWER < LOW < AVERAGE < HIGH < SUPER HIGH < ULTRA HIGH < EXTREME).

STEP 1 — INSTANT NOISE. Mark as NOISE immediately if the title is any of:
  - খেলাধুলা, বিনোদন, সেলিব্রিটি, লাইফস্টাইল, মানবিক আগ্রহের গল্প
  - কোনো ব্যক্তি, দল বা প্রতিষ্ঠানের প্রশংসা বা সমালোচনা
  - শ্রদ্ধাঞ্জলি, স্মরণ বা বার্ষিকী সংক্রান্ত লেখা
  - যেকোনো বিচ্ছিন্ন ঘটনা: একটি গ্রেপ্তার, একটি সংঘর্ষ, একটি অপরাধ, একটি দুর্ঘটনা, একটি আগুন, একটি মৃত্যু, একটি স্থানে একটি বিক্ষোভ — শিরোনাম যতই নাটকীয় হোক না কেন
  - শুধুমাত্র একটি জেলা, একটি প্রতিষ্ঠান, একটি সম্প্রদায় বা একজন ব্যক্তিকে প্রভাবিত করে এমন যেকোনো বিষয়

STEP 2 — SCOPE CHECK.

  বাংলাদেশ: SIGNAL শুধুমাত্র যদি ঘটনা বা সিদ্ধান্ত সমগ্র দেশ বা জাতীয়ভাবে গুরুত্বপূর্ণ একটি বড় অংশকে প্রভাবিত করে:
  - অর্থনৈতিক তথ্য বা সরকারি সিদ্ধান্ত: কেন্দ্রীয় ব্যাংকের পদক্ষেপ, জাতীয় বাজেট, বাণিজ্য পরিসংখ্যান, রেমিট্যান্স তথ্য, জ্বালানি/ইউটিলিটি মূল্য পরিবর্তন, বৈদেশিক মুদ্রার রিজার্ভ, মুদ্রার মান, শেয়ারবাজারে সার্কিট ব্রেকার, IMF/বিশ্বব্যাংকের বাংলাদেশ-সংক্রান্ত পদক্ষেপ
  - জাতীয় পর্যায়ে সরকার বা প্রতিষ্ঠানের পদক্ষেপ: মন্ত্রিসভার সিদ্ধান্ত, সংসদীয় আইন, দেশব্যাপী নীতি বাস্তবায়ন, সুপ্রিম কোর্টের রায়, নির্বাচন কমিশনের সিদ্ধান্ত
  - জাতীয় স্কেলে অবকাঠামো বা পাবলিক সিস্টেম: দেশব্যাপী বিদ্যুৎ বিভ্রাট, দেশজুড়ে ইন্টারনেট বিঘ্ন, একটি জাতীয় ব্যবস্থার পতন (একটি হাসপাতাল, একটি রাস্তা বা একটি কারখানা নয়)
  - জাতীয় বা বিভাগীয় স্তরে ঘোষিত প্রাকৃতিক দুর্যোগ বা স্বাস্থ্য জরুরি অবস্থা (একটি জেলা নয়)
  - বৈদেশিক বিষয়: দ্বিপক্ষীয় আলোচনা, বাংলাদেশের উপর আন্তর্জাতিক চাপ বা নিষেধাজ্ঞা, সীমান্ত-অতিক্রান্ত চুক্তি বা বিরোধ (তিস্তা, রোহিঙ্গা, বাণিজ্য), UN/IMF/WTO-তে বাংলাদেশ, বিদেশী ঋণ বা সাহায্য আনুষ্ঠানিকভাবে অনুমোদিত
  - উপ-জাতীয়, উপ-প্রতিষ্ঠানিক বা একজন ব্যক্তি সম্পর্কিত যেকোনো বিষয় → NOISE

  আন্তর্জাতিক: SIGNAL শুধুমাত্র যাচাইযোগ্য সীমান্ত-অতিক্রান্ত পরিণতি সহ কংক্রিট ঘটনার জন্য:
  - রাষ্ট্রগুলির মধ্যে সক্রিয় সশস্ত্র সংঘাত, বা যুদ্ধের আনুষ্ঠানিক ঘোষণা বা যুদ্ধবিরতি
  - বহুজাতিক সংস্থার সিদ্ধান্ত: জাতিসংঘ নিরাপত্তা পরিষদের প্রস্তাব, IMF/বিশ্বব্যাংক কর্মসূচি অনুমোদন, WTO রায়, NATO আনুষ্ঠানিক সিদ্ধান্ত, IAEA-র ফলাফল, ICC/ICJ রায়
  - আনুষ্ঠানিক বহুপাক্ষিক চুক্তি স্বাক্ষরিত বা ভেঙে পড়া
  - একটি দেশের সিদ্ধান্ত শুধুমাত্র যদি তা বিশ্ব অর্থনীতিতে সরাসরি প্রভাব ফেলে: বৈশ্বিক জ্বালানি সরবরাহ বিঘ্ন, বড় আর্থিক ব্যবস্থার পতন, পারমাণবিক অস্ত্র উন্নয়নের যাচাইকৃত মাইলফলক, তাৎক্ষণিক কার্যকর প্রভাব সহ আনুষ্ঠানিক চুক্তি প্রত্যাহার
  - যেকোনো একক বিদেশী দেশের অভ্যন্তরীণ রাজনীতি, নির্বাচন, নেতৃত্ব পরিবর্তন এবং গার্হস্থ্য নীতি → NOISE যদি না শিরোনামে সরাসরি সীমান্ত-অতিক্রান্ত পরিণতি উল্লেখ থাকে

সন্দেহ হলে → NOISE।

Output only: {{"signal": [0-based indices]}}. Valid JSON, no markdown, no explanation.

EXAMPLES:

Input:
0. যুক্তরাষ্ট্র ও চীন ঐতিহাসিক বাণিজ্য চুক্তি স্বাক্ষর করেছে
1. বাংলাদেশ ব্যাংক মূল্যস্ফীতি নিয়ন্ত্রণে সুদের হার বাড়াল
2. ঢাকা বিশ্ববিদ্যালয়ে ছাত্র সংঘর্ষ
3. জাতিসংঘ নিরাপত্তা পরিষদ সুদানে শান্তিরক্ষী মোতায়েনের পক্ষে ভোট দিয়েছে
4. নতুন বাংলাদেশের প্রতিশ্রুতি
5. বাংলাদেশ সারাদেশে জ্বালানি ভর্তুকি হ্রাস করল
6. তিস্তা পানি বণ্টন নিয়ে বাংলাদেশের পররাষ্ট্রমন্ত্রীর ভারতের সঙ্গে আলোচনা
7. কোনো একটি ক্লাবের কোচ বরখাস্ত
8. যুক্তরাষ্ট্র জিএসপি পর্যালোচনার আগে শ্রম অধিকার নিয়ে বাংলাদেশকে সতর্ক করল
9. চীন বাংলাদেশে ৩০০ কোটি ডলারের অবকাঠামো ঋণ দেওয়ার চুক্তি করল
10. টেজগাঁওয়ের কারখানায় আগুন, ৩ নিহত
11. বাংলাদেশ সংসদে নতুন সাইবার নিরাপত্তা আইন পাস
Output: {{"signal": [0, 1, 3, 5, 6, 8, 9, 11]}}

Input:
0. পাকিস্তান ও ভারত নিয়ন্ত্রণ রেখায় গোলাগুলি, হতাহতের খবর নিশ্চিত
1. ঢাকার পোশাকশ্রমিকদের ধর্মঘটে শত শত কারখানা বন্ধ
2. আইএমএফ বাংলাদেশে ৪৭০ কোটি ডলার ঋণ আনুষ্ঠানিকভাবে অনুমোদন করেছে
3. বিএনপির ভবিষ্যৎ পথচলা
4. সিলেটে ক্ষুদ্রঋণ কীভাবে জীবন বদলাচ্ছে
5. IAEA নিশ্চিত করেছে ইরান ৮৪ শতাংশ বিশুদ্ধতায় ইউরেনিয়াম সমৃদ্ধ করেছে
6. চট্টগ্রামে হত্যা মামলায় এক ব্যক্তি গ্রেপ্তার
7. বাংলাদেশের বৈদেশিক মুদ্রার রিজার্ভ ২০০ কোটি ডলারের নিচে, টাকার রেকর্ড পতন
8. প্রথম প্রান্তিকে পোশাক রপ্তানি ১২ শতাংশ কমেছে, বাংলাদেশ ব্যাংকের প্রতিবেদন
9. ICC কোনো দেশের বর্তমান রাষ্ট্রপ্রধানের বিরুদ্ধে গ্রেপ্তারি পরোয়ানা জারি করেছে
10. বাংলাদেশ সংসদে নতুন সাইবার নিরাপত্তা আইন পাস
Output: {{"signal": [0, 1, 2, 5, 7, 8, 9, 10]}}

Article titles:
{titles}
"""

DEDUP_PROMPT = """You are a news deduplication engine. Identify groups of titles covering the same story. For each group keep only the lowest index, discard the rest. Distinct topics must all be kept.

Return only the 0-based indices to KEEP as a JSON array of integers. No markdown, no preamble.

Article titles:
{titles}"""

# -- CONSTANTS -----------------------------------------------------------------

MEDIA_NS    = "http://search.yahoo.com/mrss/"
MEDIA_TAG   = "{%s}" % MEDIA_NS
ET.register_namespace("media", MEDIA_NS)

BD_TZ = timezone(timedelta(hours=6))

STATS = {
    "per_feed":              {},
    "per_method":            {"KL": 0, "DIRECT": 0},
    "total_fetched":         0,
    "total_passed_age":      0,
    "total_new":             0,
    "total_signal_mistral":  0,
    "total_signal":          0,
    "total_signal_deduped":  0,
    "timestamp":             None,
}

# -- I/O -----------------------------------------------------------------------

def load_processed_articles():
    empty = {
        "article_ids":      [],
        "article_links":    [],
        "id_timestamps":    {},
        "link_timestamps":  {},
        "last_updated":     None,
    }
    if not Path(PROCESSED_FILE).exists():
        return empty
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return empty

    cutoff = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).isoformat()

    id_ts   = {k: v for k, v in data.get("id_timestamps",   {}).items() if v >= cutoff}
    link_ts = {k: v for k, v in data.get("link_timestamps", {}).items() if v >= cutoff}

    return {
        "article_ids":      list(id_ts.keys()),
        "article_links":    list(link_ts.keys()),
        "id_timestamps":    id_ts,
        "link_timestamps":   link_ts,
        "last_updated":     data.get("last_updated"),
    }


def save_processed_articles(data):
    now_iso = datetime.utcnow().isoformat()
    cutoff  = (datetime.utcnow() - timedelta(days=RETENTION_DAYS)).isoformat()

    existing = {"id_timestamps": {}, "link_timestamps": {}}
    if Path(PROCESSED_FILE).exists():
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    id_ts   = existing.get("id_timestamps",   {})
    link_ts = existing.get("link_timestamps", {})

    for aid in data.get("article_ids", []):
        if aid and aid not in id_ts:
            id_ts[aid] = now_iso
    for lnk in data.get("article_links", []):
        if lnk and lnk not in link_ts:
            link_ts[lnk] = now_iso

    id_ts   = {k: v for k, v in id_ts.items()   if v >= cutoff}
    link_ts = {k: v for k, v in link_ts.items() if v >= cutoff}

    out = {
        "article_ids":      list(id_ts.keys()),
        "article_links":    list(link_ts.keys()),
        "id_timestamps":    id_ts,
        "link_timestamps":  link_ts,
        "last_updated":     now_iso,
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


def dedup_by_link(articles):
    seen_links = set()
    deduped = []
    for a in articles:
        link = a.get("link") or ""
        if link and link in seen_links:
            continue
        if link:
            seen_links.add(link)
        deduped.append(a)
    dropped = len(articles) - len(deduped)
    if dropped:
        print(f"Link dedup: removed {dropped} duplicate link(s) before API call.")
    return deduped

# -- CLASSIFICATION ------------------------------------------------------------

def extract_json_object(text):
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


def is_bangla_title(title):
    """Return True if the title contains Bengali Unicode characters."""
    if not title:
        return False
    return any('\u0980' <= ch <= '\u09FF' for ch in title)


def send_to_mistral(articles):
    api_key = os.environ.get("MS")
    if not api_key or not articles:
        return []

    try:
        client = Mistral(api_key=api_key)

        # Split articles into Bangla and English by index
        bangla_indices  = [i for i, a in enumerate(articles) if is_bangla_title(a.get("title", ""))]
        english_indices = [i for i, a in enumerate(articles) if not is_bangla_title(a.get("title", ""))]

        signal_indices = []

        # Classify Bangla batch
        if bangla_indices:
            bangla_articles    = [articles[i] for i in bangla_indices]
            bangla_titles_text = "\n".join([f"{j}. {a.get('title', '')}" for j, a in enumerate(bangla_articles)])
            bangla_response    = client.chat.complete(
                model=MISTRAL_MODEL,
                messages=[{"role": "user", "content": BANGLA_PROMPT.format(titles=bangla_titles_text)}],
                response_format={"type": "json_object"},
            )
            bangla_text    = bangla_response.choices[0].message.content or ""
            bangla_local   = extract_json_object(bangla_text).get("signal", [])
            # Map local indices back to original indices
            signal_indices += [bangla_indices[j] for j in bangla_local if 0 <= j < len(bangla_indices)]

        # Classify English batch
        if english_indices:
            english_articles    = [articles[i] for i in english_indices]
            english_titles_text = "\n".join([f"{j}. {a.get('title', '')}" for j, a in enumerate(english_articles)])
            english_response    = client.chat.complete(
                model=MISTRAL_MODEL,
                messages=[{"role": "user", "content": PROMPT.format(titles=english_titles_text)}],
                response_format={"type": "json_object"},
            )
            english_text    = english_response.choices[0].message.content or ""
            english_local   = extract_json_object(english_text).get("signal", [])
            # Map local indices back to original indices
            signal_indices += [english_indices[j] for j in english_local if 0 <= j < len(english_indices)]

        return sorted(signal_indices)

    except Exception as e:
        print(f"Mistral classification error: {e}")
        return []


def normalize_title_for_dedup(title):
    title = (title or "").lower().strip()
    title = re.sub(r"https?://\S+", " ", title)
    title = re.sub(r"[^a-z0-9\u0980-\u09FF]+", " ", title)
    tokens = [t for t in title.split() if t not in {
        "a", "an", "the", "of", "in", "on", "for", "to", "from", "by", "with",
        "and", "or", "at", "as", "is", "are", "was", "were", "be", "been",
        "being", "after", "before", "over", "under", "amid", "into", "about",
        "news", "report", "reports", "says", "said"
    }]
    return " ".join(tokens)


def are_near_duplicates(title_a, title_b):
    a = normalize_title_for_dedup(title_a)
    b = normalize_title_for_dedup(title_b)

    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        return True

    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens or not b_tokens:
        return False

    intersection = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    jaccard = intersection / union if union else 0.0

    seq_ratio = 0.0
    try:
        from difflib import SequenceMatcher
        seq_ratio = SequenceMatcher(None, a, b).ratio()
    except Exception:
        pass

    return jaccard >= 0.80 or seq_ratio >= 0.86


def deduplicate_signal_articles(articles):
    if not articles:
        return articles

    kept = []
    kept_titles = []

    for article in articles:
        title = article.get("title", "")
        duplicate = False
        for prev_title in kept_titles:
            if are_near_duplicates(title, prev_title):
                duplicate = True
                break
        if duplicate:
            continue
        kept.append(article)
        kept_titles.append(title)

    dropped = len(articles) - len(kept)
    if dropped:
        print(f"Dedup: removed {dropped} near-duplicate signal title(s).")
    return kept

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
    print(f"  Signal (Mistral):     {STATS['total_signal_mistral']}")
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

    new_articles = dedup_by_link(new_articles)

    STATS["total_new"] = len(new_articles)

    mistral_indices = send_to_mistral(new_articles)
    mistral_indices = [i for i in mistral_indices if 0 <= i < len(new_articles)]

    STATS["total_signal_mistral"] = len(mistral_indices)
    STATS["total_signal"]         = len(mistral_indices)

    if not mistral_indices:
        print("Mistral returned 0 signal. Skipping all file writes.")
        print_stats()
        return

    signal_articles   = [new_articles[i] for i in mistral_indices]
    excluded_articles = [new_articles[i] for i in range(len(new_articles)) if i not in set(mistral_indices)]

    print(f"Deduplicating {len(signal_articles)} signal article(s)...")
    signal_articles = deduplicate_signal_articles(signal_articles)

    STATS["total_signal_deduped"] = len(signal_articles)

    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="Curated News",
        feed_description="AI-curated signal: international affairs and Bangladesh news",
    )

    generate_xml_feed(
        excluded_articles,
        output_file=EXCLUDED_XML,
        feed_title="Excluded News",
        feed_description="Articles excluded after Mistral classification",
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
