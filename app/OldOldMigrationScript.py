def migrate_roles_and_users():
    # Get all roles
    roles = source_es.security.get_role()
    for role_name, role_body in roles.items():
        dest_es.security.put_role(name=role_name, body=role_body)
        log.info(f"Migrated role: {role_name}")

    
    # Get all users
    users = source_es.security.get_user()
    for username, user_body in users.items():
        if 'password' in user_body:
            del user_body['password']  # Passwords are hashed; cannot transfer
        dest_es.security.put_user(username=username, body=user_body)
        log.info(f"Migrated user: {username}")

    # Role mappings
    mappings = source_es.security.get_role_mapping()
    for mapping_name, mapping_body in mappings.items():
        dest_es.security.put_role_mapping(name=mapping_name, body=mapping_body)
        log.info(f"Migrated role mapping: {mapping_name}")
