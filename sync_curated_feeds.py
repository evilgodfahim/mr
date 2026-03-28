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
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime, format_datetime
from pathlib import Path

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
BD_TZ = timezone(timedelta(hours=6))   # +0600 Bangladesh Time

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
    """
    Produce a curated-style <item> string matching the format already in
    curated_feed_edit.xml / curated_feed_bdit.xml:

      <item>
        <title>...</title>
        <link>URL</link>
        <guid isPermaLink="true">URL</guid>
        <description>text</description>   or   <description />
        <pubDate>RFC-822 +0600</pubDate>
      </item>

    pubDate is set to the current curation run time (BD timezone),
    exactly as mainedit.py / mainbdit.py do it.
    """
    title_el = src_item.find("title")
    title = xml_escape(title_el.text.strip()) if (title_el is not None and title_el.text) else ""

    link_el = src_item.find("link")
    link = link_el.text.strip() if (link_el is not None and link_el.text) else ""

    desc_el = src_item.find("description")
    if desc_el is not None and desc_el.text and desc_el.text.strip():
        description = f"<description>{xml_escape(desc_el.text.strip())}</description>"
    else:
        description = "<description />"

    return "\n".join([
        "    <item>",
        f"      <title>{title}</title>",
        f"      <link>{link}</link>",
        f'      <guid isPermaLink="true">{xml_escape(link)}</guid>',
        f"      {description}",
        f"      <pubDate>{pub_now}</pubDate>",
        "    </item>",
    ])


def append_items_to_local(local_path: Path, new_items_xml: list[str]) -> int:
    """
    Insert new <item> blocks before the final </channel> tag.
    Pure string-level insert — never rewrites or truncates existing content.
    """
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
    print("  Fetching remote feed…")
    try:
        remote_root = parse_xml_text(fetch_xml_text(remote_url))
    except Exception as e:
        print(f"  [ERROR] {e}")
        sys.exit(1)

    remote_channel = remote_root.find("channel")
    if remote_channel is None:
        print("  [ERROR] No <channel> in remote feed.")
        sys.exit(1)

    # Filter to window
    recent = filter_recent_items(remote_channel, HOURS_WINDOW)
    print(f"  Remote items in last {HOURS_WINDOW}h : {len(recent)}")
    if not recent:
        print("  Nothing recent to sync.")
        return

    # Load local & get existing links
    local_tree = ET.parse(local_path)
    local_root = local_tree.getroot()
    local_channel = local_root.find("channel") or local_root
    existing = collect_existing_links(local_channel)
    print(f"  Existing items in local     : {len(existing)}")

    # Diff
    missing = [
        item for item in recent
        if (lnk := get_item_link(item)) and lnk not in existing
    ]
    print(f"  New items to append         : {len(missing)}")
    if not missing:
        print("  Already up-to-date. Nothing to do.")
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
    new_xml  = [build_curated_item(item, pub_now) for item in missing]
    count    = append_items_to_local(local_path, new_xml)
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
