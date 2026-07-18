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
WINDOW_HOURS = 48
MAX_ITEMS    = 500


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
You are a senior news editor and intelligence analyst serving a Bangladeshi readership. \
Feeds may be in Bengali or English. Any feed may contain a mix of Bangladesh national \
news, international news, economy, politics, editorial/opinion, and more.

━━ THE DEPTH TEST ━━
For every headline ask: "Does this explain a mechanism, reveal a consequence, \
or reframe understanding — or does it only announce that something happened?" \
Announcers are rejected. Explainers are selected.

━━ DEPTH SIGNALS — PREFER HEADLINES THAT ━━
  - Ask or answer WHY or HOW, not just WHAT
  - Name a mechanism, policy instrument, structural argument, or causal chain
  - Are explicitly analysis, investigation, explainer, or named editorial/opinion
  - Contain specific data, legislation names, budget figures, or named expert framing
  - Reveal something new: a document, a finding, a dataset, an exclusive source

━━ ALWAYS REJECT ━━
  - Pure announcers: "[X] does [Y]" with no mechanism or consequence explained
  - Conflict tickers: "Nth strike/attack in N days" — select an ongoing conflict \
at most once across all feeds, only if it marks genuine structural escalation
  - "[Official] says / warns / condemns / calls for" with no independent analysis
  - Wire summaries aggregating what other outlets already reported
  - Newsletters, digests, roundups, morning briefings \
("First Thing", "Morning Briefing", "Today in...", "Week in Review") — always reject
  - Routine press releases and procedural government announcements
  - Sports, entertainment — unless of direct civic consequence

━━ GEOGRAPHIC BALANCE — HARD RULE (article level, across all feeds combined) ━━
Before finalising, tag each candidate article as BD (Bangladesh national) or INT (international). \
  - BD articles must form at least one-third of all selected items total
  - INT articles are capped at two-thirds of all selected items total
  - A BD article that passes the depth test must be selected over a weaker INT article \
even if the international event is globally larger

━━ EVENT DEDUPLICATION ━━
Group ALL headlines across ALL feeds by the underlying real-world event — \
language is irrelevant. "US strikes Iran", "আমেরিকা ইরানে হামলা", \
"Washington attacks Tehran" are the same event. \
Select it ONCE from the source showing the most depth. \
Empty index for that event in every other feed. One event = one slot, total.

━━ INTERNATIONAL CATEGORY CAPS (counted across all feeds) ━━
  - International war / conflict / military: max 2 items
  - International politics / diplomacy: max 2 items
  - Any other single international topic cluster: max 2 items
BD national categories have no cap.

━━ BALANCE PER FEED ━━
For each feed: identify distinct topic categories present → pick the deepest item \
from each → fill remaining slots by depth score. \
Minimum 3 distinct categories per feed. Never fill all 5 slots from one category.

━━ OUTPUT ━━
Valid JSON only. No explanation, no markdown, no preamble. \
Keys: feed index (string). Values: array of selected headline indices \
(integers, 0-based within that feed), max 5 per feed. \
Empty array if no qualifying items.
Example: {"0": [2, 5, 11], "1": [0, 3, 9], "2": [], "3": [1, 6], "4": [4, 8, 12]}"""


def select_across_feeds(feed_titles: list[list[str]]) -> dict[int, list[int]]:
    sections = []
    for fi, titles in enumerate(feed_titles):
        lines = "\n".join(f"  {i}: {t}" for i, t in enumerate(titles))
        sections.append(f"Feed {fi}:\n{lines}")

    user_msg = (
        "Select headlines from the feeds below.\n"
        "Step 1: for each headline apply the depth test — explain or announce? Reject announcers. "
        "Step 2: tag each candidate BD or INT. "
        "Step 3: deduplicate by real-world event across all feeds. "
        "Step 4: apply international category caps. "
        "Step 5: verify BD items form at least one-third of the total selection.\n\n"
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
    ET.SubElement(ch, "title").text         = "AI Top Stories"
    ET.SubElement(ch, "link").text          = "https://evilgodfahim.github.io/mr/"
    ET.SubElement(ch, "description").text   = "Daily AI-curated top stories"
    ET.SubElement(ch, "lastBuildDate").text  = ""
    return ET.ElementTree(rss), ch, set()


def trim_to_limit(ch: ET.Element, max_items: int) -> int:
    """Remove oldest items (front of list) to stay within max_items. Returns removed count."""
    items  = ch.findall("item")
    excess = len(items) - max_items
    if excess <= 0:
        return 0
    for item in items[:excess]:
        ch.remove(item)
    return excess


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

            print(f"  {len(entries)} total | {len(unseen)} unseen | "
                  f"{len(recent)} within {WINDOW_HOURS}h "
                  f"({len(unseen) - len(recent)} too old, skipped)")

            feed_unseen.append(recent)

            # Mark ALL entries seen — old or new — so they never return
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

    # Phase 3: append to XML, enforce 500-item cap
    tree, ch, existing_links = load_or_create_tree(OUTPUT_FILE)

    added = 0
    for it in all_items:
        if it["link"] and it["link"] in existing_links:
            continue
        make_item_node(ch, it)
        existing_links.add(it["link"])
        added += 1

    recycled = trim_to_limit(ch, MAX_ITEMS)
    if recycled:
        print(f"  Recycled {recycled} oldest items to stay within {MAX_ITEMS} cap.")

    lbd = ch.find("lastBuildDate")
    if lbd is not None:
        lbd.text = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(serialise(tree))

    save_seen(seen)
    total_now = len(ch.findall("item"))
    print(f"\n✓ +{added} appended, {recycled} recycled → {total_now} items in {OUTPUT_FILE} "
          f"| seen total: {len(seen)}")


if __name__ == "__main__":
    main()
