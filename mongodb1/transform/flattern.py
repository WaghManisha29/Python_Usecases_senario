def flatten_project(project):
    client = project.get('client', {})
    location = client.get('location', {})
    return {
        "project_id": project.get("project_id"),
        "project_name": project.get("project_name"),
        "client_name": client.get("name"),
        "industry": client.get("industry"),
        "city": location.get("city"),
        "country": location.get("country"),
        "technologies": ", ".join(project.get("technologies", [])),
        "status": project.get("status"),
        "project_manager": project.get("team", {}).get("project_manager")
    }
