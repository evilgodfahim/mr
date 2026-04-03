#!/usr/bin/env python3
"""
sync_curated_feeds.py

Fetches articles from the past 26 hours from two remote filtered RSS feeds,
compares links against local curated XML files, and APPENDS (strict — never
overwrites existing content) any missing items formatted in curated style.

Feed pairs:
  filtered_feed_overflow.xml  →  curated_feed_edit.xml
  filtered_feed.xml           →  curated_feed_bdit.xml
"""

import sys
import os
import re
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime, format_datetime
from pathlib import Path
from mistralai.client import Mistral

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FEED_PAIRS = [
    {
        "remote_url": "https://evilgodfahim.github.io/gist/filtered_feed_overflow.xml",
        "local_file": "curated_feed_edit.xml",
        "label": "overflow → curated_feed_edit",
    },
    {
        "remote_url": "https://evilgodfahim.github.io/gist/filtered_feed.xml",
        "local_file": "curated_feed_bdit.xml",
        "label": "filtered → curated_feed_bdit",
    },
]

HOURS_WINDOW = 26
BD_TZ        = timezone(timedelta(hours=6))  # +0600 Bangladesh Time
DEDUP_MODEL  = "mistral-large-latest"

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_xml_text(url: str) -> str:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_xml_text(text: str) -> ET.Element:
    return ET.fromstring(text.encode("utf-8"))


def get_item_link(item: ET.Element) -> str | None:
    """Return normalised link; fallback to guid if link missing."""
    for tag in ("link", "guid"):
        el = item.find(tag)
        if el is not None and el.text and el.text.strip():
            return el.text.strip().rstrip("/")
    return None


def get_item_title(item: ET.Element) -> str:
    el = item.find("title")
    if el is not None and el.text:
        return el.text.strip()
    return ""


def get_item_pubdate(item: ET.Element) -> datetime | None:
    pd = item.find("pubDate")
    if pd is None or not pd.text:
        return None
    try:
        dt = parsedate_to_datetime(pd.text.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def collect_existing_links(channel: ET.Element) -> set[str]:
    """Collect all normalised link + guid values already in the channel."""
    links: set[str] = set()
    for item in channel.findall("item"):
        for tag in ("link", "guid"):
            el = item.find(tag)
            if el is not None and el.text and el.text.strip():
                links.add(el.text.strip().rstrip("/"))
    return links


def collect_recent_local_items(channel: ET.Element, hours: int) -> list[ET.Element]:
    """Return local items whose pubDate falls within the past N hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    recent = []
    for item in channel.findall("item"):
        pub = get_item_pubdate(item)
        if pub is not None and pub >= cutoff:
            recent.append(item)
    return recent


def filter_recent_items(channel: ET.Element, hours: int) -> list[ET.Element]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    recent = []
    no_date = 0
    for item in channel.findall("item"):
        pub = get_item_pubdate(item)
        if pub is None:
            no_date += 1
            continue
        if pub >= cutoff:
            recent.append(item)
    if no_date:
        print(f"    [WARN] {no_date} item(s) skipped — no parseable pubDate")
    return recent


def now_bd_rfc822() -> str:
    return format_datetime(datetime.now(BD_TZ))


def xml_escape(text: str) -> str:
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


def build_curated_item(src_item: ET.Element, pub_now: str) -> str:
    title_el = src_item.find("title")
    title = xml_escape(title_el.text.strip()) if (title_el is not None and title_el.text) else ""

    link_el = src_item.find("link")
    link_raw = link_el.text.strip() if (link_el is not None and link_el.text) else ""
    link_escaped = xml_escape(link_raw)

    desc_el = src_item.find("description")
    if desc_el is not None and desc_el.text and desc_el.text.strip():
        description = f"<description>{xml_escape(desc_el.text.strip())}</description>"
    else:
        description = "<description />"

    return "\n".join([
        "    <item>",
        f"      <title>{title}</title>",
        f"      <link>{link_escaped}</link>",
        f'      <guid isPermaLink="true">{link_escaped}</guid>',
        f"      {description}",
        f"      <pubDate>{pub_now}</pubDate>",
        "    </item>",
    ])


def append_items_to_local(local_path: Path, new_items_xml: list[str]) -> int:
    if not new_items_xml:
        return 0

    raw = local_path.read_text(encoding="utf-8")
    pos = raw.rfind("</channel>")
    if pos == -1:
        raise ValueError(f"</channel> not found in {local_path}")

    insert_block = "\n" + "\n".join(new_items_xml) + "\n  "
    local_path.write_text(raw[:pos] + insert_block + raw[pos:], encoding="utf-8")
    return len(new_items_xml)


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate_missing(
    missing: list[ET.Element],
    local_recent: list[ET.Element],
) -> list[ET.Element]:
    """
    Build a combined title list: local_recent titles first (anchors), then
    missing titles. Send to Mistral dedup. Return only the missing items whose
    combined indices survived.

    Index layout passed to Mistral:
      0 … len(local_recent)-1   → existing local 26h items (anchors)
      len(local_recent) … end   → incoming missing items

    Any missing item whose combined index is NOT in keep_indices is a near-
    duplicate of something already in the local XML and gets dropped.
    """
    api_key = os.environ.get("MS")
    if not api_key:
        print("    [WARN] MS not set — skipping title dedup.")
        return missing

    anchor_count = len(local_recent)
    combined     = local_recent + missing

    titles_text = "\n".join(
        f"{i}. {get_item_title(item)}" for i, item in enumerate(combined)
    )

    try:
        client = Mistral(api_key=api_key)
        response = client.chat.complete(
            model=DEDUP_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": DEDUP_PROMPT.format(titles=titles_text),
                }
            ],
            response_format={"type": "json_object"},
        )

        raw = ""
        try:
            raw = response.choices[0].message.content or ""
        except Exception:
            raw = str(response)

        raw = raw.replace("```json", "").replace("```", "").strip()

        keep_indices = None
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                keep_indices = [i for i in parsed if isinstance(i, int) and 0 <= i < len(combined)]
            elif isinstance(parsed, dict):
                for key in ("keep_indices", "indices", "keep"):
                    value = parsed.get(key)
                    if isinstance(value, list):
                        keep_indices = [i for i in value if isinstance(i, int) and 0 <= i < len(combined)]
                        break
        except Exception:
            pass

        if keep_indices is None:
            m = re.search(r"\[[\d,\s]+\]", raw)
            if m:
                try:
                    keep_indices = [
                        i for i in json.loads(m.group(0))
                        if isinstance(i, int) and 0 <= i < len(combined)
                    ]
                except Exception:
                    pass

        if keep_indices is None:
            print("    [WARN] Dedup: could not parse Mistral response — keeping all missing items.")
            return missing

        # Only care about indices that fall in the missing slice
        surviving = [
            combined[i]
            for i in sorted(set(keep_indices))
            if i >= anchor_count
        ]
        dropped = len(missing) - len(surviving)
        if dropped:
            print(f"    Dedup: removed {dropped} near-duplicate(s) against local 26h window.")
        return surviving

    except Exception as e:
        print(f"    [WARN] Dedup error: {e} — keeping all missing items.")
        return missing


# ---------------------------------------------------------------------------
# Per-pair processing
# ---------------------------------------------------------------------------

def process_pair(remote_url: str, local_file: str, label: str) -> None:
    sep = "=" * 64
    print(f"\n{sep}")
    print(f"  {label}")
    print(f"  Remote : {remote_url}")
    print(f"  Local  : {local_file}")
    print(sep)

    local_path = Path(local_file)
    if not local_path.exists():
        print(f"  [ERROR] File not found: {local_path.resolve()}")
        sys.exit(1)

    # Fetch remote
    print("  Fetching remote feed...")
    try:
        remote_root = parse_xml_text(fetch_xml_text(remote_url))
    except Exception as e:
        print(f"  [ERROR] {e}")
        sys.exit(1)

    remote_channel = remote_root.find("channel")
    if remote_channel is None:
        print("  [ERROR] No <channel> in remote feed.")
        sys.exit(1)

    # Filter remote to window
    recent = filter_recent_items(remote_channel, HOURS_WINDOW)
    print(f"  Remote items in last {HOURS_WINDOW}h : {len(recent)}")
    if not recent:
        print("  Nothing recent to sync.")
        return

    # Load local
    local_tree    = ET.parse(local_path)
    local_root    = local_tree.getroot()
    local_channel = local_root.find("channel") or local_root
    existing      = collect_existing_links(local_channel)
    print(f"  Existing items in local     : {len(existing)}")

    # Link-level dedup (exact)
    missing = []
    for item in recent:
        lnk = get_item_link(item)
        if lnk and lnk not in existing:
            missing.append(item)

    print(f"  New items (link-dedup)      : {len(missing)}")
    if not missing:
        print("  Already up-to-date. Nothing to do.")
        return

    # Collect local 26h items as anchor pool for title-level dedup
    local_recent = collect_recent_local_items(local_channel, HOURS_WINDOW)
    print(f"  Local items in 26h window   : {len(local_recent)}  (dedup anchors)")

    # Title-level dedup via Mistral
    missing = deduplicate_missing(missing, local_recent)
    print(f"  Items after title-dedup     : {len(missing)}")
    if not missing:
        print("  All new items were near-duplicates. Nothing to append.")
        return

    # Preview
    print()
    for item in missing:
        title = (item.findtext("title") or "(no title)").strip()
        pub   = (item.findtext("pubDate") or "no date").strip()
        link  = get_item_link(item) or ""
        print(f"  + [{pub}]")
        print(f"    {title[:72]}")
        print(f"    {link}")

    # Build & append
    pub_now = now_bd_rfc822()
    new_xml = [build_curated_item(item, pub_now) for item in missing]
    count   = append_items_to_local(local_path, new_xml)
    print(f"\n  ✓  Appended {count} item(s) → '{local_file}'")
    print(f"     pubDate set to: {pub_now}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    sep = "=" * 64
    print(sep)
    print("  sync_curated_feeds.py")
    print(f"  Started : {datetime.now(BD_TZ).strftime('%Y-%m-%d %H:%M:%S +0600')}")
    print(f"  Window  : last {HOURS_WINDOW} hours")
    print(sep)

    for pair in FEED_PAIRS:
        process_pair(**pair)

    print("\nDone.\n")


if __name__ == "__main__":
    main()