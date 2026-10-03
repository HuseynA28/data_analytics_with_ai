"""Simple jobs pipeline: fetch -> save -> SQLite -> summary."""
import html
import json
import re
import sqlite3
import time
import urllib.request
from contextlib import closing
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DB = DATA / "technology_jobs.db"
CACHE = DATA / "jobicy_live_cache.json"
API_URL = "https://jobicy.com/api/v2/remote-jobs?count=200"

# API field name -> database column name
COLUMNS = {
    "id": "job_id",
    "jobTitle": "job_title",
    "companyName": "company_name",
    "url": "job_url",
    "jobGeo": "job_location",
    "jobLevel": "job_level",
    "jobIndustry": "job_industry",
    "jobType": "job_type",
    "jobDescription": "job_description",
    "pubDate": "publication_date",
    "salaryMin": "salary_min",
    "salaryMax": "salary_max",
    "salaryCurrency": "salary_currency",
    "salaryPeriod": "salary_period",
}

SKILLS = ["Python", "SQL", "Snowflake", "dbt", "Dagster", "AWS", "Azure",
          "Docker", "Kubernetes", "Spark", "Kafka", "Terraform"]


# 1. Fetch the data and save it as a JSON file
def fetch_jobs(cache_path=CACHE):
    """Download the jobs and save them. Reuses the saved file if it is under 1 hour old."""
    cache_path = Path(cache_path)
    if cache_path.exists() and time.time() - cache_path.stat().st_mtime < 3600:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    request = urllib.request.Request(API_URL, headers={"User-Agent": "DataCube-Teaching/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if not payload.get("jobs"):
        raise ValueError("The API returned no jobs")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


# 2. Clean the data and save it in the SQL database
def clean_html(value):
    """Remove HTML tags and extra spaces from a piece of text."""
    if not isinstance(value, str):
        return value
    text = re.sub(r"<(script|style).*?</\1>", " ", value, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return " ".join(html.unescape(text).split())


def load_jobs(payload, db_path=DB):
    """Save the jobs in the 'jobs' table and their skills in 'job_skills'. Each run replaces both."""
    df = pd.DataFrame(payload["jobs"], dtype=object)  # dtype=object keeps IDs as 123, not 123.0
    df = df.reindex(columns=list(COLUMNS)).rename(columns=COLUMNS)
    df = df.dropna(subset=["job_id", "job_title"]).drop_duplicates("job_id", keep="last")
    df["job_id"] = df["job_id"].astype(str)
    for column in ("job_title", "company_name", "job_description"):
        df[column] = df[column].map(clean_html)
    for column in ("job_industry", "job_type"):  # lists -> "a, b, c"
        df[column] = df[column].map(lambda v: ", ".join(map(str, v)) if isinstance(v, list) else v)
    for column in ("salary_min", "salary_max"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    text = df["job_title"].fillna("") + " " + df["job_description"].fillna("")
    skills = pd.DataFrame(
        [(job_id, skill) for job_id, words in zip(df["job_id"], text) for skill in SKILLS
         if re.search(rf"(?<!\w){re.escape(skill)}(?!\w)", words, re.I)],
        columns=["job_id", "skill"])
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(db_path)) as con:
        df.to_sql("jobs", con, if_exists="replace", index=False)
        skills.to_sql("job_skills", con, if_exists="replace", index=False)
    return len(df)


def read_jobs(db_path=DB):
    """Read the jobs table back as a DataFrame."""
    with closing(sqlite3.connect(db_path)) as con:
        return pd.read_sql_query("SELECT * FROM jobs", con)


# 3. Build a small summary (this is what the AI will see)
def build_summary(df):
    text = df["job_title"].fillna("") + " " + df["job_description"].fillna("")
    skills = {s: int(text.str.contains(rf"(?<!\w){re.escape(s)}(?!\w)", case=False).sum())
              for s in SKILLS}
    paid = df.dropna(subset=["salary_min", "salary_max", "salary_currency", "salary_period"])
    midpoint = (paid["salary_min"] + paid["salary_max"]) / 2
    group = paid["salary_currency"].str.upper() + " " + paid["salary_period"].str.lower()
    return {
        "jobs": len(df),
        "companies": int(df["company_name"].nunique()),
        "top_companies": df["company_name"].value_counts().head(5).to_dict(),
        "skills": dict(sorted(skills.items(), key=lambda item: -item[1])),
        "jobs_with_salary": len(paid),
        "median_salary": midpoint.groupby(group).median().round().to_dict(),
    }


if __name__ == "__main__":
    print("Saved to database:", load_jobs(fetch_jobs()), "jobs")
    print(json.dumps(build_summary(read_jobs()), indent=2))
