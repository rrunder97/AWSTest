from elasticsearch import Elasticsearch, helpers
import logging
import re

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Source and destination Elasticsearch instances
SOURCE_ES = "http://source-es-host:9200"
DEST_ES = "https://your-elasticsearch-service-hostname:443"

# Authentication (if required)
SRC_AUTH = ("elastic", "source_password")
DEST_AUTH = ("elastic", "destination_password")

# Create clients
source_es = Elasticsearch(SOURCE_ES, basic_auth=SRC_AUTH)
dest_es = Elasticsearch(DEST_ES, basic_auth=DEST_AUTH)

def get_all_indices():
    return list(source_es.indices.get_alias("*").keys())

def get_index_mapping(index):
    return source_es.indices.get_mapping(index=index)[index]['mappings']

def get_ilm_policy(index):
    try:
        settings = source_es.indices.get_settings(index=index)[index]['settings']
        ilm_name = settings.get('index', {}).get('lifecycle', {}).get('name')
        return ilm_name
    except Exception:
        return None

def copy_component_templates():
    try:
        templates = source_es.cluster.get_component_template()
        for name, value in templates.get("component_templates", {}).items():
            body = value["component_template"]
            if not dest_es.cluster.get_component_template(name=name, ignore_unavailable=True):
                dest_es.cluster.put_component_template(name=name, body=body)
                log.info(f"Copied component template: {name}")
            else:
                log.info(f"Template '{name}' already exists in destination.")
    except Exception as e:
        log.error(f"Error copying component templates: {e}")

def migrate_indices():
    indices = get_all_indices()

    for index in indices:
        log.info(f"\n--- Processing index: {index} ---")

        # Normalize index naming for special patterns
        if index.lower().startswith("cipher-obligations-"):
            base_name = "cipher-obligations"
            new_index_name = f"{base_name}-v1"
            alias_name = base_name
        else:
            new_index_name = index
            alias_name = None

        # Get mappings
        try:
            mapping = get_index_mapping(index)
        except Exception as e:
            log.error(f"Error fetching mapping for {index}: {e}")
            continue

        # Create destination index
        if not dest_es.indices.exists(index=new_index_name):
            try:
                dest_es.indices.create(index=new_index_name, mappings=mapping)
                log.info(f"Created index: {new_index_name}")
            except Exception as e:
                log.error(f"Failed to create index {new_index_name}: {e}")
                continue
        else:
            log.info(f"Index {new_index_name} already exists")

        # Reindex documents
        try:
            docs = helpers.scan(source_es, index=index, preserve_order=True, query={"query": {"match_all": {}}})
            actions = (
                {
                    "_index": new_index_name,
                    "_source": doc["_source"]
                }
                for doc in docs
            )
            helpers.bulk(dest_es, actions)
            log.info(f"Reindexed documents from {index} to {new_index_name}")
        except Exception as e:
            log.error(f"Error reindexing {index}: {e}")
            continue

        # Create alias if required
        if alias_name:
            try:
                dest_es.indices.put_alias(index=new_index_name, name=alias_name)
                log.info(f"Created alias '{alias_name}' → '{new_index_name}'")
            except Exception as e:
                log.warning(f"Could not create alias '{alias_name}': {e}")

        # ILM policy copy (log only, actual ILM creation requires extra steps)
        ilm_policy = get_ilm_policy(index)
        if ilm_policy:
            log.info(f"Index {index} uses ILM policy: {ilm_policy}")
        else:
            log.info(f"No ILM policy attached to index: {index}")

def main():
    log.info("Starting migration process...")
    copy_component_templates()
    migrate_indices()
    log.info("Migration completed.")

if __name__ == "__main__":
    main()
