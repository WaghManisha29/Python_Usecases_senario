def map_customer_profiles(projects):
    profiles = {}

    for proj in projects:
        client = proj.get("client", "Unknown")
        if not isinstance(client, str):
            client = str(client)

        if client not in profiles:
            profiles[client] = {
                "client": client,
                "domain": proj.get("domain"),
                "location": proj.get("location"),
                "technologies": set(proj.get("technologies", [])),
                "total_projects": 1
            }
        else:
            profiles[client]["technologies"].update(proj.get("technologies", []))
            profiles[client]["total_projects"] += 1

    # Format technologies for CSV
    for profile in profiles.values():
        profile["technologies"] = ", ".join(sorted(profile["technologies"]))

    return list(profiles.values())
