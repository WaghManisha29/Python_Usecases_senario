from extract.mongo_connector import connect_to_mongodb, fetch_documents
from transform.flattern import flatten_project
from transform.cleaner import extract_team_members, extract_milestones
from load.writer import save_to_csv

def main():
    print(" Connecting to MongoDB...")
    collection = connect_to_mongodb()
    raw_data = fetch_documents(collection)

    print(f" Fetched {len(raw_data)} documents")

    # Transform
    flat_projects = [flatten_project(p) for p in raw_data]
    team_members = [m for p in raw_data for m in extract_team_members(p)]
    milestones = [m for p in raw_data for m in extract_milestones(p)]

    # Load
    save_to_csv(flat_projects, "output/projects.csv")
    save_to_csv(team_members, "output/team_members.csv")
    save_to_csv(milestones, "output/milestones.csv")

    print(" ETL completed. CSVs saved in /output")

if __name__ == "__main__":
    main()
