import os
import json
import re
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import feedparser
from mistralai import Mistral

FEEDS = [
    "https://evilgodfahim.github.io/mr/curated_feed.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_bdit.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_edit.xml",
    "https://evilgodfahim.github.io/mr/curated_feed_gp.xml",
    "https://evilgodfahim.github.io/mr/curated_feedb.xml",
]

OUTPUT_FILE = "top_stories.xml"
MODEL = "mistral-large-latest"

client = Mistral(api_key=os.environ["MS"])


def select_indices(titles: list[str]) -> list[int]:
    """Send numbered titles to Mistral, get back 5 most significant indices."""
    if len(titles) <= 5:
        return list(range(len(titles)))

    numbered = "\n".join(f"{i}: {t}" for i, t in enumerate(titles))
    prompt = (
        "You are a news editor selecting the most important stories of the day.\n"
        "From the numbered headlines below, pick exactly 5 that are most significant "
        "by news value, impact, and relevance to a general audience.\n"
        "Return ONLY a JSON array of 5 integers (the index numbers). "
        "No explanation, no text, nothing else.\n\n"
        f"{numbered}"
    )

    resp = client.chat.complete(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    raw = resp.choices[0].message.content.strip()

    match = re.search(r"\[[\d\s,]+\]", raw)
    if not match:
        raise ValueError(f"Could not parse index array from Mistral response: {raw!r}")

    indices = json.loads(match.group())
    # Validate: must be ints within bounds
    valid = [i for i in indices if isinstance(i, int) and 0 <= i < len(titles)]
    if not valid:
        raise ValueError(f"No valid indices in: {indices}")
    return valid[:5]


def make_item_node(ch: ET.Element, it: dict) -> ET.Element:
    node = ET.SubElement(ch, "item")
    ET.SubElement(node, "title").text = it.get("title", "")
    ET.SubElement(node, "link").text = it.get("link", "")
    ET.SubElement(node, "description").text = it.get("summary", "")
    if it.get("published"):
        ET.SubElement(node, "pubDate").text = it["published"]
    ET.SubElement(node, "source").text = it.get("feed_url", "")
    return node


def load_or_create_tree(path: str) -> tuple[ET.ElementTree, ET.Element, set[str]]:
    """Return (tree, channel_element, set_of_existing_links)."""
    if os.path.exists(path):
        tree = ET.parse(path)
        ch = tree.getroot().find("channel")
        existing = {item.findtext("link", "") for item in ch.findall("item")}
        return tree, ch, existing

    # First run — build skeleton
    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = "AI Top Stories"
    ET.SubElement(ch, "link").text = "https://evilgodfahim.github.io/mr/"
    ET.SubElement(ch, "description").text = "Daily AI-curated top stories across all feeds"
    ET.SubElement(ch, "lastBuildDate").text = ""
    return ET.ElementTree(rss), ch, set()


def serialise(tree: ET.ElementTree) -> str:
    ET.indent(tree.getroot(), space="  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
        tree.getroot(), encoding="unicode"
    )


def main():
    all_items = []

    for url in FEEDS:
        print(f"→ Fetching: {url}")
        try:
            feed = feedparser.parse(url)
            entries = feed.entries

            if not entries:
                print("  No entries found, skipping.")
                continue

            titles = [e.get("title", f"[untitled {i}]") for i, e in enumerate(entries)]
            print(f"  {len(titles)} entries found.")

            indices = select_indices(titles)
            print(f"  Selected indices: {indices}")

            for idx in indices:
                e = entries[idx]
                all_items.append({
                    "title":    e.get("title", ""),
                    "link":     e.get("link", ""),
                    "summary":  e.get("summary", ""),
                    "published": e.get("published", ""),
                    "feed_url": url,
                })

        except Exception as exc:
            print(f"  ERROR: {exc}")

    tree, ch, existing_links = load_or_create_tree(OUTPUT_FILE)

    # Append only items not already in the file (deduplicate by link)
    added = 0
    for it in all_items:
        if it["link"] and it["link"] in existing_links:
            print(f"  skip duplicate: {it['title'][:60]}")
            continue
        make_item_node(ch, it)
        existing_links.add(it["link"])
        added += 1

    # Update lastBuildDate
    lbd = ch.find("lastBuildDate")
    if lbd is not None:
        lbd.text = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(serialise(tree))

    print(f"\n✓ {added} new items appended to {OUTPUT_FILE} ({len(existing_links)} total)")


if __name__ == "__main__":
    main()
