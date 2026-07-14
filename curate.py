import os
import json
import re
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import feedparser
import requests

FEEDS = [
    "https://evilgodfahim.github.io/mr/curated_feed.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_bdit.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_edit.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_gp.xml",
    "https://evilgodfahim.github.io/mr/curated_feedb.xml",
]

OUTPUT_FILE = "top_stories.xml"
SEEN_FILE   = "seen.json"
MODEL       = "mistral-large-latest"


# ── seen.json ────────────────────────────────────────────────────────────────

def load_seen() -> set[str]:
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen(seen: set[str]) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(seen), f, indent=2, ensure_ascii=False)


# ── AI selection ─────────────────────────────────────────────────────────────

def select_across_feeds(feed_titles: list[list[str]]) -> dict[int, list[int]]:
    """
    Single Mistral call across all feeds.
    Returns {feed_index: [selected_indices_within_that_feed]}.
    Cross-feed semantic deduplication is done by the model — works across languages.
    """
    sections = []
    for fi, titles in enumerate(feed_titles):
        lines = "\n".join(f"  {i}: {t}" for i, t in enumerate(titles))
        sections.append(f"Feed {fi}:\n{lines}")

    prompt = (
        "You are a news editor reviewing headlines from multiple feeds.\n"
        "Some feeds may be in different languages but cover the same stories.\n\n"
        "Instructions:\n"
        "1. For each feed, select up to 5 most significant headlines by news value and impact.\n"
        "2. Cross-feed deduplication: if the same story appears across feeds "
        "(even in different languages), select it from ONE feed only — whichever covers it best. "
        "Do not select it again in any other feed.\n\n"
        "Return ONLY a valid JSON object. Keys are feed indices (strings), "
        "values are arrays of selected headline indices (integers) within that feed. "
        "Each array has at most 5 items. Feeds with no worthy or non-duplicate headlines "
        "may have fewer than 5.\n"
        "No explanation, no markdown, no extra text.\n\n"
        "Example format: {\"0\": [2, 5, 8], \"1\": [0, 3, 7, 9, 12]}\n\n"
        + "\n\n".join(sections)
    )

    resp = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['MS']}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
        },
        timeout=60,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()

    # Parse — try direct first, then extract from surrounding text
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            raise ValueError(f"Could not parse JSON from Mistral response: {raw!r}")
        result = json.loads(match.group())

    # Validate indices per feed
    validated: dict[int, list[int]] = {}
    for fi, titles in enumerate(feed_titles):
        raw_indices = result.get(str(fi), [])
        valid = [i for i in raw_indices if isinstance(i, int) and 0 <= i < len(titles)]
        validated[fi] = valid[:5]

    return validated


# ── XML helpers ───────────────────────────────────────────────────────────────

def make_item_node(ch: ET.Element, it: dict) -> None:
    node = ET.SubElement(ch, "item")
    ET.SubElement(node, "title").text = it.get("title", "")
    ET.SubElement(node, "link").text = it.get("link", "")
    ET.SubElement(node, "description").text = it.get("summary", "")
    if it.get("published"):
        ET.SubElement(node, "pubDate").text = it["published"]
    ET.SubElement(node, "source").text = it.get("feed_url", "")


def load_or_create_tree(path: str) -> tuple[ET.ElementTree, ET.Element, set[str]]:
    if os.path.exists(path):
        tree = ET.parse(path)
        ch = tree.getroot().find("channel")
        existing = {item.findtext("link", "") for item in ch.findall("item")}
        return tree, ch, existing

    rss = ET.Element("rss", version="2.0")
    ch  = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text       = "AI Top Stories"
    ET.SubElement(ch, "link").text        = "https://evilgodfahim.github.io/mr/"
    ET.SubElement(ch, "description").text = "Daily AI-curated top stories across all feeds"
    ET.SubElement(ch, "lastBuildDate").text = ""
    return ET.ElementTree(rss), ch, set()


def serialise(tree: ET.ElementTree) -> str:
    ET.indent(tree.getroot(), space="  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
        tree.getroot(), encoding="unicode"
    )


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    seen = load_seen()

    # Phase 1: fetch all feeds, collect unseen entries
    feed_unseen: list[list]  = []   # unseen entries per feed (parallel to FEEDS)

    for url in FEEDS:
        print(f"→ Fetching: {url}")
        try:
            entries = feedparser.parse(url).entries
            if not entries:
                print("  No entries.")
                feed_unseen.append([])
                continue

            unseen = [e for e in entries if e.get("link", "") not in seen]
            print(f"  {len(entries)} entries, {len(unseen)} unseen.")
            feed_unseen.append(unseen)

            # Mark ALL entries seen now — rejected articles won't return either
            for e in entries:
                seen.add(e.get("link", ""))

        except Exception as exc:
            print(f"  ERROR: {exc}")
            feed_unseen.append([])

    # Phase 2: single cross-feed AI call (skip if nothing new)
    active = [(fi, unseen) for fi, unseen in enumerate(feed_unseen) if unseen]

    all_items: list[dict] = []

    if not active:
        print("\nNo new articles across any feed — skipping AI call.")
    else:
        feed_titles = [
            [e.get("title", f"[untitled {i}]") for i, e in enumerate(unseen)]
            for _, unseen in active
        ]

        print(f"\nSending {sum(len(t) for t in feed_titles)} titles across "
              f"{len(active)} feeds to Mistral (cross-feed dedup enabled)…")

        try:
            selections = select_across_feeds(feed_titles)
        except Exception as exc:
            print(f"AI error: {exc}")
            selections = {}

        for local_fi, (original_fi, unseen) in enumerate(active):
            url     = FEEDS[original_fi]
            indices = selections.get(local_fi, [])
            print(f"  Feed {original_fi}: selected {indices}")
            for idx in indices:
                e = unseen[idx]
                all_items.append({
                    "title":     e.get("title", ""),
                    "link":      e.get("link", ""),
                    "summary":   e.get("summary", ""),
                    "published": e.get("published", ""),
                    "feed_url":  url,
                })

    # Phase 3: append to XML
    tree, ch, existing_links = load_or_create_tree(OUTPUT_FILE)

    added = 0
    for it in all_items:
        if it["link"] and it["link"] in existing_links:
            continue
        make_item_node(ch, it)
        existing_links.add(it["link"])
        added += 1

    lbd = ch.find("lastBuildDate")
    if lbd is not None:
        lbd.text = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(serialise(tree))

    save_seen(seen)
    print(f"\n✓ {added} new items appended → {OUTPUT_FILE}  |  seen total: {len(seen)}")


if __name__ == "__main__":
    main()
