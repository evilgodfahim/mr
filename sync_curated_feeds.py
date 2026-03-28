#!/usr/bin/env python3
"""
sync_curated_feeds.py

Fetches articles from the past 26 hours from two remote filtered RSS feeds,
compares links against local curated XML files, and APPENDS (strict — never
overwrites existing content) any missing items formatted in curated style.

Feed pairs:
  filtered_feed_overflow.xml  →  curated_feed_edit.xml
  filtered_feed.xml           →  curated_feed_bdit.xml

Curated item format produced:
  <item>
    <title>...</title>
    <link>...</link>
    <guid isPermaLink="true">...</guid>
    <description>...</description>   (or self-closing if empty)
    <pubDate>NOW in +0600</pubDate>
  </item>
"""

import sys
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime, format_datetime
from pathlib import Path
from copy import deepcopy

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

HOURS_WINDOW = 26          # look-back window in hours
BD_TZ = timezone(timedelta(hours=6))   # +0600 Bangladesh Time

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_xml_text(url: str) -> str:
    """Download a feed and return the raw text."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    # Ensure correct encoding (some feeds declare UTF-8 but respond as latin-1)
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_xml_text(text: str) -> ET.Element:
    return ET.fromstring(text.encode("utf-8"))


def get_item_link(item: ET.Element) -> str | None:
    """Return the stripped, normalised link of an <item>."""
    link_el = item.find("link")
    if link_el is not None and link_el.text:
        return link_el.text.strip().rstrip("/")
    # Fallback: check guid
    guid_el = item.find("guid")
    if guid_el is not None and guid_el.text:
        return guid_el.text.strip().rstrip("/")
    return None


def get_item_pubdate(item: ET.Element) -> datetime | None:
    """Parse <pubDate> and return UTC-aware datetime, or None."""
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
    """Collect all normalised links (and guids) already in a channel."""
    links: set[str] = set()
    for item in channel.findall("item"):
        for tag in ("link", "guid"):
            el = item.find(tag)
            if el is not None and el.text:
                links.add(el.text.strip().rstrip("/"))
    return links


def filter_recent_items(channel: ET.Element, hours: int) -> list[ET.Element]:
    """Return items whose pubDate falls within the last `hours` hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    recent = []
    skipped_no_date = 0
    for item in channel.findall("item"):
        pub = get_item_pubdate(item)
        if pub is None:
            skipped_no_date += 1
            continue
        if pub >= cutoff:
            recent.append(item)
    if skipped_no_date:
        print(f"    [WARN] {skipped_no_date} item(s) skipped — no parseable pubDate")
    return recent


def now_bd_rfc822() -> str:
    """Return current BD time (+0600) formatted as RFC 822 for pubDate."""
    now_bd = datetime.now(BD_TZ)
    return format_datetime(now_bd)


def build_curated_item(src_item: ET.Element, pub_now: str) -> str:
    """
    Build a curated-style <item> XML string from a gist feed item.

    Curated format:
      <item>
        <title>...</title>
        <link>...</link>
        <guid isPermaLink="true">LINK</guid>
        <description>...</description>  or  <description />
        <pubDate>pub_now</pubDate>
      </item>

    pubDate is set to the curation run time (current BD time), matching
    how the existing curated items are generated.
    """
    def esc(text: str) -> str:
        """Escape XML special characters."""
        return (text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;"))

    title_el = src_item.find("title")
    title = esc(title_el.text.strip()) if (title_el is not None and title_el.text) else ""

    link_el = src_item.find("link")
    link = (link_el.text.strip() if (link_el is not None and link_el.text) else "")

    desc_el = src_item.find("description")
    if desc_el is not None and desc_el.text and desc_el.text.strip():
        description = f"<description>{esc(desc_el.text.strip())}</description>"
    else:
        description = "<description />"

    # guid uses the link URL
    guid = link

    lines = [
        "  <item>",
        f"    <title>{title}</title>",
        f"    <link>{link}</link>",
        f'    <guid isPermaLink="true">{esc(guid)}</guid>',
        f"    {description}",
        f"    <pubDate>{pub_now}</pubDate>",
        "  </item>",
    ]
    return "\n".join(lines)


def append_items_to_local(local_path: Path, new_items_xml: list[str]) -> int:
    """
    Strictly append formatted <item> strings before the final </channel>
    in the local file. Never truncates or replaces existing content.
    Returns number of items appended.
    """
    if not new_items_xml:
        return 0

    raw = local_path.read_text(encoding="utf-8")

    insert_block = "\n" + "\n".join(new_items_xml) + "\n"

    pos = raw.rfind("</channel>")
    if pos == -1:
        raise ValueError(f"</channel> not found in {local_path}")

    new_raw = raw[:pos] + insert_block + raw[pos:]
    local_path.write_text(new_raw, encoding="utf-8")
    return len(new_items_xml)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def process_pair(remote_url: str, local_file: str, label: str) -> None:
    print(f"\n{'='*64}")
    print(f"  {label}")
    print(f"  Remote : {remote_url}")
    print(f"  Local  : {local_file}")
    print(f"{'='*64}")

    local_path = Path(local_file)
    if not local_path.exists():
        print(f"  [ERROR] Local file not found: {local_path.resolve()}")
        sys.exit(1)

    # 1. Fetch and parse remote feed
    print("  Fetching remote feed...")
    try:
        remote_text = fetch_xml_text(remote_url)
        remote_root = parse_xml_text(remote_text)
    except Exception as e:
        print(f"  [ERROR] Failed to fetch/parse remote feed: {e}")
        sys.exit(1)

    remote_channel = remote_root.find("channel")
    if remote_channel is None:
        print("  [ERROR] No <channel> in remote feed.")
        sys.exit(1)

    # 2. Filter to last HOURS_WINDOW hours
    recent_items = filter_recent_items(remote_channel, HOURS_WINDOW)
    print(f"  Remote items in last {HOURS_WINDOW}h : {len(recent_items)}")
    if not recent_items:
        print("  Nothing recent. Done.")
        return

    # 3. Load local feed and get existing links
    local_tree = ET.parse(local_path)
    local_root = local_tree.getroot()
    local_channel = local_root.find("channel")
    if local_channel is None:
        local_channel = local_root  # fallback if root IS channel

    existing_links = collect_existing_links(local_channel)
    print(f"  Existing items in local     : {len(existing_links)}")

    # 4. Find missing items
    missing: list[ET.Element] = []
    for item in recent_items:
        link = get_item_link(item)
        if not link:
            print("  [SKIP] Item has no link/guid.")
            continue
        if link.rstrip("/") not in existing_links:
            missing.append(item)

    print(f"  New items to append         : {len(missing)}")
    if not missing:
        print("  Local feed already up-to-date.")
        return

    # 5. Log what will be appended
    print()
    for item in missing:
        title = (item.findtext("title") or "(no title)").strip()
        link  = get_item_link(item) or "(no link)"
        pub   = (item.findtext("pubDate") or "(no date)").strip()
        print(f"  + [{pub}]")
        print(f"    {title[:72]}")
        print(f"    {link}")

    # 6. Build curated-style XML strings (single pubDate for this run)
    pub_now = now_bd_rfc822()
    new_items_xml = [build_curated_item(item, pub_now) for item in missing]

    # 7. Append strictly (no overwrite)
    appended = append_items_to_local(local_path, new_items_xml)
    print(f"\n  ✓ Appended {appended} item(s) to '{local_file}' at {pub_now}")


def main() -> None:
    print(f"{'='*64}")
    print(f"  sync_curated_feeds.py")
    print(f"  Started : {datetime.now(BD_TZ).strftime('%Y-%m-%d %H:%M:%S +0600')}")
    print(f"  Window  : last {HOURS_WINDOW} hours")
    print(f"{'='*64}")

    for pair in FEED_PAIRS:
        process_pair(**pair)

    print(f"\n  All done.")


if __name__ == "__main__":
    main()
