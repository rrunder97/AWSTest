from elasticsearch import Elasticsearch
from fnmatch import fnmatch
import json
import time

# === Configuration ===
SOURCE_ES = "http://source-es-url:9200"  # Source Elasticsearch instance URL
TARGET_ES = "http://target-es-url:9200"  # Target Elasticsearch instance URL
prefix = "migrated-"  # Prefix to append to migrated indices (you can customize)

# Optional: Auth - replace with your ES credentials if needed
es_source = Elasticsearch(SOURCE_ES, basic_auth=("user", "pass"))
es_target = Elasticsearch(TARGET_ES, basic_auth=("user", "pass"))

# === Utility Functions ===

# def create_index_if_no_template_Verrsion1(index_name, new_index_name):
#     """Create the target index manually if no matching template exists in the target ES."""
#     try:
#         # Get all templates in target Elasticsearch
#         templates = es_target.indices.get_index_template(name='*')
#         matched = False
#         # Check if the current index matches any templates
#         for tpl in templates.get("index_templates", []):
#             patterns = tpl["index_template"]["index_patterns"]
#             if any(fnmatch(new_index_name, pat) for pat in patterns):
#                 matched = True
#                 break

#         # If no matching template found, create the index manually with the same settings and mappings
#         if not matched:
#             print(f"No matching index template for '{new_index_name}'. Creating manually.")
#             settings = es_source.indices.get_settings(index=index_name)[index_name]["settings"]["index"]
#             mappings = es_source.indices.get_mapping(index=index_name)[index_name]["mappings"]
#             filtered_settings = {
#                 k: v for k, v in settings.items()
#                 if not k.startswith("version") and not k.startswith("uuid") and not k.startswith("provided_name")
#             }
#             # Create the new index on the target with filtered settings and mappings
#             es_target.indices.create(
#                 index=new_index_name,
#                 body={"settings": filtered_settings, "mappings": mappings}
#             )
#             print(f"Created index '{new_index_name}' manually.")
#     except Exception as e:
#         print(f" Error creating index '{new_index_name}': {e}")


#the index exists with the appropriate settings, mappings, aliases, and ILM policies. 
#This function doesn't migrate any data itself, but it ensures that the new index is properly created before the data migration takes place.
def create_index_if_no_template(es_source, es_target, index_name, new_index_name):
    # Get the list of index templates from the source environment
    templates = es_source.indices.get_template(name=index_name, flat_settings=True) 

    matched = False  # Flag to track if a matching template is found
    
    for template_name, template_data in templates.items():
        if template_name == index_name:
            matched = True  # Matching template found
            print(f"Found matching template for '{index_name}'. Using template settings and mappings.")
            
            # Extract the settings and mappings from the template
            settings = template_data['settings']['index']
            mappings = template_data['mappings']
            aliases = template_data.get('aliases', {})
            
            # Filter out irrelevant settings like version, uuid, and provided_name
            filtered_settings = {
                k: v for k, v in settings.items()
                if not k.startswith("version") and not k.startswith("uuid") and not k.startswith("provided_name")
            }

            # Create the new index with the extracted settings and mappings
            create_index_payload = {
                'settings': filtered_settings,
                'mappings': mappings,
                'aliases': aliases
            }
            
            es_target.indices.create(index=new_index_name, body=create_index_payload)
            print(f"Index '{new_index_name}' created using the template '{template_name}'.")
            break

    if not matched:
        print(f"No matching template found for '{new_index_name}'. Creating manually with the same settings and mappings.")
        
        # If no matching template found, get the current settings and mappings from the source index
        settings = es_source.indices.get_settings(index=index_name)[index_name]["settings"]["index"]
        mappings = es_source.indices.get_mapping(index=index_name)[index_name]["mappings"]
        
        # Filter settings to remove unnecessary fields
        filtered_settings = {
            k: v for k, v in settings.items()
            if not k.startswith("version") and not k.startswith("uuid") and not k.startswith("provided_name")
        }
        
        # Create the new index manually with the filtered settings and mappings
        create_index_payload = {
            'settings': filtered_settings,
            'mappings': mappings
        }
        
        es_target.indices.create(index=new_index_name, body=create_index_payload)
        print(f"Index '{new_index_name}' created manually from source index settings and mappings.")
        
        # Check if any aliases exist on the source index, and apply them to the new index
        aliases = es_source.indices.get(index_name).get(index_name, {}).get('aliases', {})
        if aliases:
            es_target.indices.put_alias(index=new_index_name, name=aliases)
            print(f"Aliases applied to the new index '{new_index_name}'.")

    # Handle Index Lifecycle Management (ILM) if applicable
    ilm_policy = es_source.indices.get_settings(index=index_name)[index_name].get('settings', {}).get('index.lifecycle.name')
    if ilm_policy:
        print(f"Applying ILM policy '{ilm_policy}' to the new index '{new_index_name}'.")
        es_target.indices.put_settings(index=new_index_name, body={
            "settings": {
                "index.lifecycle.name": ilm_policy
            }
        })
    else:
        print(f"No ILM policy found for index '{index_name}'.")



def migrate_component_templates():
    """Migrate component templates from source to target Elasticsearch."""
    try:
        # Fetch all component templates from source ES
        templates = es_source.cluster.get_component_template()
        for tmpl in templates.get("component_templates", []):
            name = tmpl["name"]
            body = tmpl["component_template"]
            # Create the component template on the target
            es_target.cluster.put_component_template(name=name, body=body)
            print(f" Migrated component template '{name}'")
    except Exception as e:
        print(f"Error migrating component templates: {e}")


def migrate_index_templates():
    """Migrate index templates from source to target Elasticsearch."""
    try:
        # Fetch all index templates from source ES
        templates = es_source.indices.get_index_template()
        for tpl in templates.get("index_templates", []):
            name = tpl["name"]
            body = tpl["index_template"]
            # Create the index template on the target
            es_target.indices.put_index_template(name=name, body=body)
            print(f"Migrated index template '{name}'")
    except Exception as e:
        print(f"Error migrating index templates: {e}")


def migrate_ilm_policies():
    """Migrate Index Lifecycle Management (ILM) policies from source to target Elasticsearch."""
    try:
        # Fetch ILM policies from source ES
        policies = es_source.ilm.get_lifecycle()
        for name, body in policies.items():
            # Create the ILM policy on the target
            es_target.ilm.put_lifecycle(name=name, policy=body['policy'])
            print(f"Migrated ILM policy '{name}'")
    except Exception as e:
        print(f"Error migrating ILM policies: {e}")


def migrate_indices():
    """Migrate indices from source to target Elasticsearch, including reindexing data."""
    # Get all indices in the source Elasticsearch that aren't system indices
    indices = [i for i in es_source.indices.get_alias("*") if not i.startswith('.')]
    for index_name in indices:
        new_index_name = f"{prefix}{index_name}"  # Add a prefix to the index name

        # Ensure the target index exists or is created manually if needed
        create_index_if_no_template(index_name, new_index_name)

        # Define the reindex operation from the source index to the new index on the target
        reindex_body = {
            "source": {"index": index_name},
            "dest": {"index": new_index_name}
        }
        try:
            # Perform the reindex operation (copy documents to the new index)
            es_target.reindex(body=reindex_body, wait_for_completion=True, request_timeout=300)
            print(f"Reindexed '{index_name}' → '{new_index_name}'")
        except Exception as e:
            print(f"Failed to reindex '{index_name}': {e}")


def migrate_roles():
    """Migrate roles from source to target Elasticsearch."""
    try:
        # Fetch all roles from source ES
        roles = es_source.security.get_role()
        for role, body in roles.items():
            # Create the role on the target
            es_target.security.put_role(role, body)
            print(f"Migrated role '{role}'")
    except Exception as e:
        print(f"Error migrating roles: {e}")


def clone_role_with_new_pattern(existing_role, new_role):
    """Clone an existing role, but modify the index pattern."""
    try:
        # Fetch the role definition from source ES
        role = es_source.security.get_role(name=existing_role)[existing_role]
        # Modify the role's index pattern to only match *cyberops* indices
        new_indices = []
        for idx_perm in role.get("indices", []):
            idx_perm["names"] = [f"*cyberops*"]
            new_indices.append(idx_perm)
        role["indices"] = new_indices
        # Create the new role on the target with the modified pattern
        es_target.security.put_role(name=new_role, body=role)
        print(f"Cloned role '{existing_role}' → '{new_role}' with updated index pattern")
    except Exception as e:
        print(f"Error cloning role: {e}")


def view_and_clone_role(existing_role, new_role):
    """View the existing role and clone it to the new role in the target ES."""
    # Fetch the role from source ES
    role = es_source.security.get_role(name=existing_role)
    # Pretty print the role definition
    print(json.dumps(role, indent=2))
    # Apply the role to the target environment
    es_target.security.put_role(name=new_role, body=role[existing_role])
    print(f" Applied role '{existing_role}' as '{new_role}' in target environment")




# === Main Function: Run Everything ===
def main():
    print("🚀 Starting Elasticsearch migration...\n")
    
    # Step 1: Migrate component templates (doesn't need index names)
    migrate_component_templates()
    
    # Step 2: Migrate index templates (doesn't need index names)
    migrate_index_templates()
    
    # Step 3: Migrate ILM policies (doesn't need index names)
    migrate_ilm_policies()
    
    # Step 4: Migrate roles (doesn't need index names)
    migrate_roles()
    
    # Fetching all index names from the source cluster
    index_names = es_source.cat.indices(format="json")
    index_names = [index['index'] for index in index_names]
    
    # Step 5: Migrate data with reindexing (including ensuring index exists if no template found)
    for index_name in index_names:
        create_index_if_no_template(index_name)  # Ensuring the index is created with settings/mappings if no template exists
        migrate_indices(index_name)  # Reindex data
    
    # Optional: Clone a specific role with a new pattern
    # clone_role_with_new_pattern("existing_role_name", "new_role_cyberops")
    
    print("\n🎉 Migration completed!")


# === Run the Migration ===
if __name__ == "__main__":
    main()





# === SSO Notes ===
# SecureAuth or another SSO provider:
# - Your SSO configuration (SSO/SAML/OIDC) must be handled outside of this script
# - This script does not configure SSO but helps create roles that would map to your identity provider's roles.
