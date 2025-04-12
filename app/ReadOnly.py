#Read Only
#!/usr/bin/env python3
import re
import json
from elasticsearch import Elasticsearch

# === CONFIGURATION ===
# Change these to point at your cluster and credentials:
SOURCE_ES = "http://localhost:9200"
USER      = "user"
PASS      = "pass"

# === INDEX SELECTION ===
# Option A: Wildcard pattern (Elasticsearch-style, e.g. "*test*", "logs-2025.*")
WILDCARD_PATTERN = "*test*"

# Option B: Python regex (e.g. r"^prod-.*-2025\.\d\d$")
# If you set this to a non-empty string, it takes precedence over wildcard.
REGEX_PATTERN = None

# How many sample docs to show per index
SAMPLE_SIZE = 3

# === CLIENT ===
es = Elasticsearch(
    SOURCE_ES,
    basic_auth=(USER, PASS)
)

# ————————————————————————————————————————————————
# 1) Listing helpers
# ————————————————————————————————————————————————
def list_indices_by_wildcard(es: Elasticsearch, pattern: str) -> list:
    raw = es.cat.indices(
        index=pattern,
        format="json",
        expand_wildcards="all"
    )
    return [idx["index"] for idx in raw if not idx["index"].startswith(".")]

def list_indices_by_regex(es: Elasticsearch, regex: str) -> list:
    raw = es.cat.indices(format="json", expand_wildcards="all")
    pat = re.compile(regex)
    return [
        idx["index"] for idx in raw
        if not idx["index"].startswith(".") and pat.search(idx["index"])
    ]

# ————————————————————————————————————————————————
# 2) Quick‑print overview helper
# ————————————————————————————————————————————————
def print_index_info(es: Elasticsearch, index: str, sample_size: int = 3):
    print(f"\n=== INDEX: {index} ===")

    # Settings
    settings = es.indices.get_settings(
        index=index, expand_wildcards="all"
    )[index]["settings"]["index"]
    print("\n• Settings:")
    print(json.dumps(settings, indent=2))

    # Mappings
    mapping = es.indices.get_mapping(
        index=index, expand_wildcards="all"
    )[index]["mappings"]
    print("\n• Mappings:")
    print(json.dumps(mapping, indent=2))

    # Aliases
    aliases = es.indices.get_alias(
        index=index, expand_wildcards="all"
    )[index].get("aliases", {})
    print("\n• Aliases:", list(aliases.keys()))

    # ILM policy name
    ilm_name = settings.get("index.lifecycle.name")
    print("\n• ILM Policy:", ilm_name or "(none)")

    # Sample documents
    print(f"\n• Sample {sample_size} docs:")
    resp = es.search(
        index=index,
        size=sample_size,
        body={"query": {"match_all": {}}},
        _source_includes=["*"],
    )
    for doc in resp["hits"]["hits"]:
        print(json.dumps(doc["_source"], indent=2))

# ————————————————————————————————————————————————
# 3) Main entrypoint
# ————————————————————————————————————————————————
def main():
    if REGEX_PATTERN:
        indices = list_indices_by_regex(es, REGEX_PATTERN)
        print(f"🔍 Found {len(indices)} indices matching regex '{REGEX_PATTERN}'")
    else:
        indices = list_indices_by_wildcard(es, WILDCARD_PATTERN)
        print(f"🔍 Found {len(indices)} indices matching wildcard '{WILDCARD_PATTERN}'")

    if not indices:
        print("⚠️  No indices matched your pattern.")
        return

    for idx in indices:
        print_index_info(es, idx, SAMPLE_SIZE)

if __name__ == "__main__":
    main()
