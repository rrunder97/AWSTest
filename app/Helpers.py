#!/usr/bin/env python3
import json
from elasticsearch import Elasticsearch

# === Configuration ===
SOURCE_ES = "http://source-es-url:9200"
AUTH = {"user": "user", "pass": "pass"}

# === Client ===
es = Elasticsearch(SOURCE_ES, basic_auth=(AUTH["user"], AUTH["pass"]))

# === Helpers from before (trimmed for brevity) ===



# System indices are internal Elasticsearch indices that the cluster uses to store its own metadata and operational data. They always start with a dot (.), for example:
# .security-7 (security settings)
# .kibana_1 (Kibana saved objects)
# .watcher-history-8-000001 (Watcher history)
# You almost never want to migrate these with your application data. That’s why in our helper we filter them out:

def list_indices(pattern="*"):
    """
    Return all non‑system indices matching `pattern` (open + closed).
    """
    raw = es.cat.indices(
        format="json",
        expand_wildcards="all",
        index=pattern
    )
    return [i["index"] for i in raw if not i["index"].startswith(".")]

def get_index_settings(es, index):
    return es.indices.get_settings(index=index, expand_wildcards="all")[index]["settings"]["index"]

def get_index_mapping(es, index):
    return es.indices.get_mapping(index=index, expand_wildcards="all")[index]["mappings"]

def get_index_aliases(es, index):
    aliases = es.indices.get_alias(index=index, expand_wildcards="all")[index].get("aliases", {})
    return list(aliases.keys())

def get_ilm_policy_for_index(es, index):
    settings = get_index_settings(es, index)
    policy = settings.get("index.lifecycle.name")
    if not policy:
        return {}
    return es.ilm.get_lifecycle(name=policy).get(policy, {})

import fnmatch
def list_index_templates_for_index(es, index):
    all_tpls = es.indices.get_index_template().get("index_templates", [])
    return [tpl for tpl in all_tpls
            if any(fnmatch.fnmatch(index, pat)
                   for pat in tpl["index_template"]["index_patterns"])]

def list_component_templates_for_index(es, index):
    # find index-templates that match
    itpls = list_index_templates_for_index(es, index)
    used = {ct for tpl in itpls for ct in tpl["index_template"].get("composed_of", [])}
    all_ct = {c["name"]: c["component_template"]
              for c in es.cluster.get_component_template().get("component_templates", [])}
    return [{ "name": name, "body": all_ct[name] } for name in used if name in all_ct]

def list_all_ingest_pipelines(es):
    return es.ingest.get_pipeline()

def list_snapshot_repositories(es):
    return es.snapshot.get_repository()

def list_transforms_for_index(es, index):
    return [t for t in es.transform.get_transform().get("transforms", [])
            if index in t["config"]["source"]["index"]]

def list_rollup_jobs_for_index(es, index):
    return [j for j in es.rollup.get_jobs().get("jobs", [])
            if j["config"]["index_pattern"] == index]

def list_watcher_watches(es):
    return es.watcher.get_watch()

def list_enrich_policies(es):
    return es.enrich.get_policy().get("policies", [])

# === Main: inspect all *test* indices ===

if __name__ == "__main__":


#     So if you just call:

# python
# Copy
# all_idxs = list_indices()
# print(all_idxs)
# you’ll see every index in the cluster. If you want only those containing "test", do:

# python
# Copy
# test_idxs = list_indices("*test*")
# print(test_idxs)


    pattern = "*test*"
    indices = list_indices(pattern)
    print(f"Found {len(indices)} indices matching '{pattern}': {indices}\n")

    for idx in indices:
        print(f"\n=== INDEX: {idx} ===\n")

        print("• Settings:")
        print(json.dumps(get_index_settings(es, idx), indent=2))

        print("\n• Mappings:")
        print(json.dumps(get_index_mapping(es, idx), indent=2))

        print("\n• Aliases:", get_index_aliases(es, idx))

        print("\n• ILM Policy:")
        ilm = get_ilm_policy_for_index(es, idx)
        print(json.dumps(ilm, indent=2) if ilm else "  (none)")

        print("\n• Matching Index Templates:")
        its = list_index_templates_for_index(es, idx)
        print(json.dumps(its, indent=2) if its else "  (none)")

        print("\n• Component Templates Used:")
        cts = list_component_templates_for_index(es, idx)
        print(json.dumps(cts, indent=2) if cts else "  (none)")

        print("\n• Ingest Pipelines (all):")
        print(json.dumps(list_all_ingest_pipelines(es), indent=2))

        print("\n• Snapshot Repositories:")
        print(json.dumps(list_snapshot_repositories(es), indent=2))

        print("\n• Transforms on this index:")
        print(json.dumps(list_transforms_for_index(es, idx), indent=2) or "  (none)")

        print("\n• Rollup Jobs on this index:")
        print(json.dumps(list_rollup_jobs_for_index(es, idx), indent=2) or "  (none)")

        print("\n• Watcher Watches (all):")
        print(json.dumps(list_watcher_watches(es), indent=2))

        print("\n• Enrich Policies (all):")
        print(json.dumps(list_enrich_policies(es), indent=2))

        print("\n" + "="*60 + "\n")
