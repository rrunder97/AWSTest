#!/usr/bin/env python3
import time
import logging
import fnmatch
import json
from elasticsearch import Elasticsearch, exceptions

# === Configuration ===
SOURCE_ES = "http://source-es-url:9200"
TARGET_ES = "http://target-es-url:9200"
AUTH = {"user": "user", "pass": "pass"}
PREFIX = "migrated-"
SLICE_COUNT = 4
BATCH_SIZE = 1000
REQUEST_TIMEOUT = 600
THROTTLE_DOCS_PER_SEC = -1  # -1 = no throttle

# === Logging Setup ===
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("es_migration")


# === CLIENTS ===
es_source = Elasticsearch(
    SOURCE_ES,
    basic_auth=(AUTH["user"], AUTH["pass"])
)
es_target = Elasticsearch(
    TARGET_ES,
    basic_auth=(AUTH["user"], AUTH["pass"])
)


# === Utility Functions ===
def list_indices():
    """
    Return all non‑system indices (open + closed) from the source cluster.
    """
    raw = es_source.cat.indices(format="json", expand_wildcards="all")
    return [idx["index"] for idx in raw if not idx["index"].startswith(".")]

def create_index_if_no_template(es_source, es_target, index_name, new_index_name):
    """
    Ensure `new_index_name` exists on target with same settings/mappings/aliases as
    `index_name` on source—either via an index template or by copying directly.
    """
    try:
        # 1) Try to find a matching index-template on source
        templates = es_source.indices.get_index_template().get("index_templates", [])
        for tpl in templates:
            patterns = tpl["index_template"]["index_patterns"]
            if any(fnmatch.fnmatch(index_name, pat) for pat in patterns):
                logger.info("🧩 Using template '%s' for index '%s'", tpl["name"], index_name)
                tmpl = tpl["index_template"]["template"]
                # filter out version/uuid/provided_name
                settings = {
                    k: v for k, v in tmpl["settings"].get("index", {}).items()
                    if not k.startswith(("version","uuid","provided_name"))
                }
                body = {
                    "settings": settings,
                    "mappings": tmpl.get("mappings", {}),
                    "aliases": tmpl.get("aliases", {})
                }
                es_target.indices.create(index=new_index_name, body=body)
                return
        # 2) No template → copy settings & mappings directly
        logger.info("⚙️  No template match for '%s'; copying settings/mappings manually", index_name)
        src_settings = es_source.indices.get_settings(index=index_name)[index_name]["settings"]["index"]
        settings = {
            k: v for k, v in src_settings.items()
            if not k.startswith(("version","uuid","provided_name"))
        }
        mappings = es_source.indices.get_mapping(index=index_name)[index_name]["mappings"]
        body = {"settings": settings, "mappings": mappings}
        es_target.indices.create(index=new_index_name, body=body)
        # copy any aliases
        aliases = es_source.indices.get(index=index_name)[index_name].get("aliases", {})
        for alias in aliases:
            es_target.indices.put_alias(index=new_index_name, name=alias)
        logger.info("✅ Created '%s' manually with aliases %s", new_index_name, list(aliases))
    except exceptions.ElasticsearchException as e:
        logger.error("Error creating index '%s': %s", new_index_name, e)

def swap_aliases(es_client, old_index, new_index, aliases):
    """Atomically move each alias in `aliases` from old_index → new_index."""
    actions = [
        {"add": {"index": new_index, "alias": alias}}
        for alias in aliases
    ]
    es.indices.update_aliases(body={"actions": actions})


# === Cluster‑level Migrations ===

def migrate_component_templates():
    try:
        resp = es_source.cluster.get_component_template()
        for tmpl in resp.get("component_templates", []):
            name = tmpl["name"]
            body = tmpl["component_template"]
            es_target.cluster.put_component_template(name=name, body=body)
            logger.info("📦 Migrated component template '%s'", name)
    except Exception as e:
        logger.error("Error migrating component templates: %s", e)

def migrate_index_templates():
    try:
        resp = es_source.indices.get_index_template()
        for tpl in resp.get("index_templates", []):
            name = tpl["name"]
            body = tpl["index_template"]
            es_target.indices.put_index_template(name=name, body=body)
            logger.info("📦 Migrated index template '%s'", name)
    except Exception as e:
        logger.error("Error migrating index templates: %s", e)

def migrate_ingest_pipelines():
    try:
        pipelines = es_source.ingest.get_pipeline()
        for pid, body in pipelines.items():
            es_target.ingest.put_pipeline(id=pid, body=body)
            logger.info("🚰 Migrated ingest pipeline '%s'", pid)
    except Exception as e:
        logger.error("Error migrating ingest pipelines: %s", e)

def migrate_stored_scripts():
    try:
        scripts = es_source.cluster.get_stored_script()
        for lang, slist in scripts.items():
            for sid, body in slist.items():
                es_target.cluster.put_stored_script(id=sid, body=body)
                logger.info("✒️  Migrated stored script '%s'", sid)
    except Exception as e:
        logger.error("Error migrating stored scripts: %s", e)

def migrate_ilm_policies():
    try:
        policies = es_source.ilm.get_lifecycle()
        for name, body in policies.items():
            es_target.ilm.put_lifecycle(name=name, policy=body["policy"])
            logger.info("🕒 Migrated ILM policy '%s'", name)
    except Exception as e:
        logger.error("Error migrating ILM policies: %s", e)

def migrate_roles():
    try:
        roles = es_source.security.get_role()
        for role, body in roles.items():
            es_target.security.put_role(name=role, body=body)
            logger.info("🔐 Migrated role '%s'", role)
    except Exception as e:
        logger.error("Error migrating roles: %s", e)

def migrate_users():
    try:
        users = es_source.security.get_user()
        for user, body in users.items():
            es_target.security.put_user(username=user, body=body)
            logger.info("👤 Migrated user '%s'", user)
    except Exception as e:
        logger.error("Error migrating users: %s", e)

def migrate_role_mappings():
    try:
        mappings = es_source.security.get_role_mapping()
        for name, body in mappings.items():
            es_target.security.put_role_mapping(name=name, body=body)
            logger.info("🔗 Migrated role mapping '%s'", name)
    except Exception as e:
        logger.error("Error migrating role mappings: %s", e)

def migrate_transforms():
    try:
        transforms = es_source.transform.get_transform()
        for t in transforms.get("transforms", []):
            tid = t["id"]
            cfg = t["config"]
            es_target.transform.put_transform(transform_id=tid, body=cfg)
            logger.info("🔄 Migrated transform '%s'", tid)
    except Exception as e:
        logger.error("Error migrating transforms: %s", e)

def migrate_rollup_jobs():
    try:
        jobs = es_source.rollup.get_jobs().get("jobs", [])
        for job in jobs:
            cfg = job["config"]
            jid = cfg["id"]
            es_target.rollup.put_job(id=jid, body=cfg)
            logger.info("📊 Migrated rollup job '%s'", jid)
    except Exception as e:
        logger.error("Error migrating rollup jobs: %s", e)

def migrate_watchers():
    try:
        watches = es_source.watcher.get_watch()
        for wid, body in watches.items():
            es_target.watcher.put_watch(id=wid, body=body["watch"])
            logger.info("🔔 Migrated watcher '%s'", wid)
    except Exception as e:
        logger.error("Error migrating watcher watches: %s", e)

def migrate_enrich_policies():
    try:
        policies = es_source.enrich.get_policy().get("policies", [])
        for pol in policies:
            name = pol["name"]
            es_target.enrich.put_policy(name=name, body=pol)
            logger.info("🌾 Migrated enrich policy '%s'", name)
    except Exception as e:
        logger.error("Error migrating enrich policies: %s", e)

# === Data Migration ===

def migrate_index(es_source, es_target, index_name):
    new_index = f"{PREFIX}{index_name}"
    # 1) create target index if needed
    create_index_if_no_template(es_source, es_target, index_name, new_index)

    # 2) kick off remote, sliced reindex
    body = {
        "source": {
            "remote": {
                "host":     SOURCE_ES,
                "username": AUTH["user"],
                "password": AUTH["pass"]
            },
            "index": index_name,
            "size":  BATCH_SIZE
        },
        "dest": {"index": new_index},
        "slices": SLICE_COUNT,
        "requests_per_second": THROTTLE_DOCS_PER_SEC
    }

    try:
        resp = es_target.reindex(
            body=body,
            wait_for_completion=False,
            request_timeout=REQUEST_TIMEOUT
        )
        task_id = resp["task"]
        logger.info("🚀 Started reindex task %s for %s → %s", task_id, index_name, new_index)

        # 3) poll until complete
        while True:
            status = es_target.tasks.get(task_id=task_id)
            if status.get("completed"):
                break
            stats = status["task"]["status"]
            logger.info(
                "   Progress: %d/%d docs",
                stats["created"], stats["total"]
            )
            time.sleep(30)

        # 4) check failures
        failures = status["task"]["status"].get("failures", [])
        if failures:
            logger.warning(
                "❗ Reindex of '%s' completed with %d failures",
                index_name, len(failures)
            )
        else:
            logger.info(
                "✅ Reindex of '%s' complete (%d docs)",
                index_name, status["task"]["status"]["created"]
            )

        # 5) swap aliases
        src_aliases = list(
            es_source.indices.get_alias(index=index_name)[index_name]["aliases"].keys()
        )
        if src_aliases:
            swap_aliases(es_target, index_name, new_index, src_aliases)

    except exceptions.TransportError as e:
        logger.error("TransportError during reindex of '%s': %s", index_name, e.info)
    except Exception as e:
        logger.error("Unexpected error during reindex of '%s': %s", index_name, e)

# === Main Orchestration ===
def main():
    logger.info("🔄 Starting full Elasticsearch migration")
    # Cluster‑level
    migrate_component_templates()
    migrate_index_templates()
    migrate_ingest_pipelines()
    migrate_stored_scripts()
    migrate_ilm_policies()
    migrate_roles()
    migrate_users()
    migrate_role_mappings()
    migrate_transforms()
    migrate_rollup_jobs()
    migrate_watchers()
    migrate_enrich_policies()

    # Data
    indices = es_source.cat.indices(format="json")
    for idx in indices:
        name = idx["index"]
        if name.startswith("."):
            continue
        migrate_index(es_source, es_target, name)

    logger.info("🎉 Migration completed successfully")

if __name__ == "__main__":
    main()
