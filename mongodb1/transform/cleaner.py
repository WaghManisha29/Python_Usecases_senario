def extract_team_members(project):
    members = project.get("team", {}).get("members", [])
    return [
        {
            "project_id": project["project_id"],
            "member_name": member["name"],
            "role": member["role"]
        }
        for member in members
    ]

def extract_milestones(project):
    return [
        {
            "project_id": project["project_id"],
            "milestone_name": m["name"],
            "due_date": m["due_date"]
        }
        for m in project.get("milestones", [])
    ]
