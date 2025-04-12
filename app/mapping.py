from elasticsearch import Elasticsearch
import json

# === configure your client ===
es = Elasticsearch(
    "http://source-es-url:9200",
    basic_auth=("user", "pass")
)

def print_index_mapping(es_client, index_name):
    """
    Fetches and prints the mappings for `index_name`.
    """
    # Get the full mapping response
    resp = es_client.indices.get_mapping(index=index_name, expand_wildcards="all")
    
    # Drill into the mappings for your index
    mappings = resp.get(index_name, {}).get("mappings", {})
    
    # Pretty-print as JSON
    print(f"Mappings for index '{index_name}':\n")
    print(json.dumps(mappings, indent=2))

if __name__ == "__main__":
    print_index_mapping(es, "your-index-name")