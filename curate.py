import os
import json
import re
from datetime import datetime, timezone, timedelta
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

OUTPUT_FILE  = "top_stories.xml"
SEEN_FILE    = "kakalala.json"
MODEL        = "mistral-large-latest"
WINDOW_HOURS = 24


# ── seen.json ─────────────────────────────────────────────────────────────────

def load_seen() -> set[str]:
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def save_seen(seen: set[str]) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(seen), f, indent=2, ensure_ascii=False)


# ── time filter ───────────────────────────────────────────────────────────────

def is_within_window(entry) -> bool:
    """True if published within WINDOW_HOURS. If date missing, include by default."""
    parsed = getattr(entry, "published_parsed", None)
    if parsed is None:
        return True
    try:
        pub_dt = datetime(*parsed[:6], tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - pub_dt <= timedelta(hours=WINDOW_HOURS)
    except Exception:
        return True


# ── AI selection ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a senior news editor. Select the most significant and balanced headlines \
from the feeds you receive. Feeds may be in any language.

RULES — apply strictly in this order:

1. SIGNIFICANCE
   Judge each story by three axes:
   - Consequence: how many people are affected and how severely
   - Durability: lasting structural impact vs. one-day story
   - Depth: original reporting or analysis vs. wire summary of the same event

   Prefer:
   - Policy decisions, legislation, governance changes
   - Economic developments with structural implications (not daily market moves)
   - High-consequence international or conflict developments
   - Editorials and analysis that frame an important debate or offer genuine insight

   Avoid:
   - Routine updates, press releases, and procedural news
   - Wire-service summaries when original reporting on the same event is in the feed
   - Celebrity, sports, entertainment — unless of unusual civic significance

2. BALANCE
   Picks from each feed must span different topic categories.
   Algorithm: identify the major categories present in the feed → pick the single \
strongest headline from each → fill remaining slots strictly by significance.
   Never fill all 5 slots from one category even if it dominates the feed.

3. CROSS-FEED DEDUPLICATION
   If the same story appears across feeds in any language, select it from ONE feed \
only — the one with the most original or detailed coverage. \
Return an empty index list for that story in all other feeds.

4. OUTPUT
   Return ONLY a valid JSON object. No explanation, no markdown, no preamble.
   Keys: feed index as string. Values: array of selected headline indices (integers, \
0-based within that feed), at most 5. Empty array if no qualifying headlines.
   Example: {"0": [2, 5, 8, 11], "1": [0, 3, 7, 9, 12], "2": []}"""


def select_across_feeds(feed_titles: list[list[str]]) -> dict[int, list[int]]:
    sections = []
    for fi, titles in enumerate(feed_titles):
        lines = "\n".join(f"  {i}: {t}" for i, t in enumerate(titles))
        sections.append(f"Feed {fi}:\n{lines}")

    user_msg = "Select headlines from the following feeds:\n\n" + "\n\n".join(sections)

    resp = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ['MS']}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            "temperature": 0.0,
        },
        timeout=60,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            raise ValueError(f"Could not parse JSON from Mistral response: {raw!r}")
        result = json.loads(match.group())

    validated: dict[int, list[int]] = {}
    for fi, titles in enumerate(feed_titles):
        raw_indices = result.get(str(fi), [])
        valid = [i for i in raw_indices if isinstance(i, int) and 0 <= i < len(titles)]
        validated[fi] = valid[:5]

    return validated


# ── XML helpers ───────────────────────────────────────────────────────────────

def make_item_node(ch: ET.Element, it: dict) -> None:
    node = ET.SubElement(ch, "item")
    ET.SubElement(node, "title").text       = it.get("title", "")
    ET.SubElement(node, "link").text        = it.get("link", "")
    ET.SubElement(node, "description").text = it.get("summary", "")
    if it.get("published"):
        ET.SubElement(node, "pubDate").text = it["published"]
    ET.SubElement(node, "source").text      = it.get("feed_url", "")


def load_or_create_tree(path: str) -> tuple[ET.ElementTree, ET.Element, set[str]]:
    if os.path.exists(path):
        tree = ET.parse(path)
        ch   = tree.getroot().find("channel")
        existing = {item.findtext("link", "") for item in ch.findall("item")}
        return tree, ch, existing

    rss = ET.Element("rss", version="2.0")
    ch  = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text        = "AI Top Stories"
    ET.SubElement(ch, "link").text         = "https://evilgodfahim.github.io/mr/"
    ET.SubElement(ch, "description").text  = "Daily AI-curated top stories"
    ET.SubElement(ch, "lastBuildDate").text = ""
    return ET.ElementTree(rss), ch, set()


def serialise(tree: ET.ElementTree) -> str:
    ET.indent(tree.getroot(), space="  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
        tree.getroot(), encoding="unicode"
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    seen = load_seen()

    # Phase 1: fetch all feeds, collect unseen + recent entries
    feed_unseen: list[list] = []

    for url in FEEDS:
        print(f"→ Fetching: {url}")
        try:
            entries = feedparser.parse(url).entries
            if not entries:
                print("  No entries.")
                feed_unseen.append([])
                continue

            unseen = [e for e in entries if e.get("link", "") not in seen]
            recent = [e for e in unseen if is_within_window(e)]

            skipped_old  = len(unseen) - len(recent)
            print(f"  {len(entries)} total | {len(unseen)} unseen | "
                  f"{len(recent)} within {WINDOW_HOURS}h "
                  f"({skipped_old} too old, skipped)")

            feed_unseen.append(recent)

            # Mark ALL entries seen — old or new, so they never return
            for e in entries:
                seen.add(e.get("link", ""))

        except Exception as exc:
            print(f"  ERROR: {exc}")
            feed_unseen.append([])

    # Phase 2: single cross-feed AI call
    active = [(fi, unseen) for fi, unseen in enumerate(feed_unseen) if unseen]

    all_items: list[dict] = []

    if not active:
        print("\nNo new articles within window — skipping AI call.")
    else:
        feed_titles = [
            [e.get("title", f"[untitled {i}]") for i, e in enumerate(unseen)]
            for _, unseen in active
        ]
        total = sum(len(t) for t in feed_titles)
        print(f"\nSending {total} titles across {len(active)} feeds to Mistral…")

        try:
            selections = select_across_feeds(feed_titles)
        except Exception as exc:
            print(f"AI error: {exc}")
            selections = {}

        for local_fi, (original_fi, unseen) in enumerate(active):
            url     = FEEDS[original_fi]
            indices = selections.get(local_fi, [])
            print(f"  Feed {original_fi}: selected indices {indices}")
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
