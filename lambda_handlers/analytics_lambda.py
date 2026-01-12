import os, json, urllib.parse
import pandas as pd
import fsspec


BLS_BUCKET = os.environ["BUCKET"]
BLS_KEY = os.environ.get("BLS_KEY", "bls/pr/pr.data.0.Current")
TARGET_SERIES = os.environ.get("TARGET_SERIES_ID", "PRS30006032")
TARGET_PERIOD = os.environ.get("TARGET_PERIOD", "Q01")
START_YEAR = int(os.environ.get("START_YEAR", "2013"))
END_YEAR = int(os.environ.get("END_YEAR", "2018"))

def load_bls_df(bls_path: str):
    bls_df = pd.read_csv(bls_path,sep="\t",dtype=str,compression="gzip")
    return(bls_df)

def load_population_usa_df(population_usa_path: str):
    with fsspec.open(population_usa_path, "r") as f:
        payload = json.load(f)
    population_usa_df = pd.json_normalize(payload["data"])
    return(population_usa_df)

def bls_population_data_cleanse(bls_df, population_usa_df):
    bls_df.columns = bls_df.columns.str.strip().str.lower()
    bls_df["series_id"] = bls_df["series_id"].str.strip()
    bls_df["period"] = bls_df["period"].str.strip()
    bls_df["year"] = bls_df["year"].astype(str).str.strip().astype(int)
    bls_df["value"] = bls_df["value"].astype(float)

    # ---- Clean population data ----
    population_usa_df.columns = population_usa_df.columns.str.strip().str.lower()
    population_usa_df["year"] = population_usa_df["year"].astype(str).str.strip().astype(int)
    population_usa_df["population"] = population_usa_df["population"].astype(float)

    return(bls_df, population_usa_df)

def population_usa_stats(pop_df, start_year, end_year):
    population_usa_stats_df = (
        pop_df[(pop_df["year"] >= start_year) & (pop_df["year"] <= end_year)]
        .agg(
            mean_population=("population", "mean"),
            std_population=("population", "std"),
        )
    )
    return(population_usa_stats_df)

def best_year_df(bls_df):
    best_year_df = (
        bls_df
        .groupby(["series_id", "year"], as_index=False)["value"]
        .sum()
        .sort_values(["series_id", "value"], ascending=[True, False])
        .drop_duplicates("series_id")
    )
    return (best_year_df)

def generate_bls_population_report(bls_df, pop_df, series_id, quarter):
    target_df = (bls_df[(bls_df["series_id"] == series_id) & (bls_df["period"] == quarter)]
                 .merge(pop_df[["year", "population"]],on="year",how="left")
                 .sort_values("year"))
    return(target_df)

def handler(event, context):
    bls_path = f"s3://{BLS_BUCKET}/{BLS_KEY}"
    bls_df = load_bls_df(bls_path)
    for r in event.get("Records", []):
        body = json.loads(r["body"])
        if "Message" in body:
            body = json.loads(body["Message"])

        s3rec = body["Records"][0]["s3"]
        pop_bucket = s3rec["bucket"]["name"]
        pop_key = urllib.parse.unquote_plus(s3rec["object"]["key"])
        pop_path = f"s3://s3-quest-bls-dataset-neelima/demographics"

        pop_df = load_population_usa_df(pop_path)
        bls_cleansed_df, population_usa_cleansed_df = bls_population_data_cleanse(bls_df.copy(), pop_df)

        # Report 1: mean/std pop 2013-2018
        population_usa_stats_df = population_usa_stats(population_usa_cleansed_df,START_YEAR,END_YEAR)
        print(f"Mean population: {population_usa_stats_df.loc['mean_population', 'population']:,.0f}")
        print(f"Std deviation: {population_usa_stats_df.loc['std_population', 'population']:,.0f}\n")

        # Report 2: best year per series_id
        best_series_id_year_df = best_year_df(bls_cleansed_df)
        print("Best year with max value for all quarters for all series_id\n")
        print(best_series_id_year_df)

        # Report 3: PRS30006032 Q01 with population
        report_df = generate_bls_population_report(bls_cleansed_df,population_usa_cleansed_df,TARGET_SERIES,TARGET_PERIOD)
        print("Report to get population for target series_id and period \n")
        print(report_df[["series_id", "year", "period", "value", "population"]])

    return {"ok": True}