def transform(df):
    df = df.drop(columns=["_id"], errors="ignore")
    df["technologies"] = df["technologies"].apply(lambda x: [str(t) for t in x] if isinstance(x, list) else [])
    return df
