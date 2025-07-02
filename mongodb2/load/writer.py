import pandas as pd

def save_profiles_to_csv(profiles, filepath="output/customer_profiles.csv"):
    df = pd.DataFrame(profiles)
    df.to_csv(filepath, index=False)
    print(f" Customer profiles saved to {filepath}")
