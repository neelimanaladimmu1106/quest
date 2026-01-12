# BLS Data Pipeline – AWS & Analytics Quest

## Overview
This project implements an end-to-end **data engineering pipeline** using AWS services.  
It demonstrates dataset ingestion, API integration, data analytics, and full automation using Infrastructure as Code.

The solution is divided into **four parts**, aligned exactly with the quest requirements.
## Part 1: AWS S3 & Dataset Synchronization (BLS)
- Source: BLS Time Series datasets (PR series)
- Script: `html_to_s3_loader.py`
- Output: Files stored in S3 under `bls/pr/`. Please find below the pre-signed s3 url which will expire in 7 days.
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.class?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T014731Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=c302529ef45d5a2f9a882232e8a2979ae2a0734a0aab8a14379810b4d2dd054a
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.contacts?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T014839Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=480f0c5c2dff589bf3914446749fb403901cfbb47bec22ef0367ee8265a5dc0a
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.data.0.Current?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T014933Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=cdd56fb187c4d4110e62293684764c67c7c9da41ae01168715a56f57fb17c2b0
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.data.1.AllData?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T014955Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=a357437ac2762c39fad7de8018d4173638b34f4ca1d0068d32ad91385c0e89d7
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.duration?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015022Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=22f414908a2f6ae4d05bf24d555497556b4eeff760ee9f5b468732484820ca60
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.footnote?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015054Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=15702db73d305cbeb698865d851786e58f565173e9eafe518563d822371983ad
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.measure?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015120Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=162bdc8b4cfb304e28c33efabb735d86996f9b8ef2cff815f5e135fe515fd748
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.period?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015140Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=acda41bec693c3475592b39ad9892a79ff79690bec5fce8ff66a3cce59031804
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.seasonal?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015200Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=6ca4cb06cc54c028abf3a1e6cb26982824b8718b23917e91d5bffdd8c44f6a54
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.sector?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015234Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=e31e295b3972ee0f2011eb6e8960852acea1c87d2a3c92b15856bdfee623bf1a
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.series?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015255Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=99f13a403adb6d2b09684c87cfa988b65b82a4a73dfa7e91032ec5243a945d32
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/bls/pr/pr.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015313Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=187116f1eff92a426eca23dc36fc25369961ea687048b14edf004f4044b98bec

## Part 2: API Integration (Population Data)
- Source: Fetches US population data from the provided API
- Script: `json_to_s3_loader.py`
- Output: JSON files stored under `demographics/` prefix in S3. Please find below the pre-signed s3 url which will expire in 7 days.
https://s3-quest-bls-dataset-neelima.s3.us-east-2.amazonaws.com/demographics?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARVUKAUCZ676RG67J%2F20260112%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20260112T015350Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=457c65b01f4e682ea85d6a2788caccb379ffbc8e2e533998cdee92e769f98806

## Part 3: Data Analytics
- Source: Data from part 1 and part 2
- Script: `report_analysis.ipynb`
- Output: Data cleansing, Population statistics, Best year per BLS series, Joined report to get population for target seriesId and period.

## Part 4: Automation & Infrastructure as Code (AWS CDK)
This is to automate the above tasks using AWS CDK service.
- File: `infra/infra_stack.py`

## Resources Created
- Amazon S3 (private bucket)
- AWS Lambda:
    - **Ingest Lambda** (Part 1 + Part 2)
    - **Analytics Lambda** (Part 3)
- Amazon SQS
- Amazon EventBridge (daily schedule)

##  Note on Tooling & Approach
For the Infrastructure-as-Code and Lambda implementation, I leveraged AI-assisted development tools to accelerate delivery and follow AWS best practices.

While my primary experience is in data engineering, ingestion, and analytics, I intentionally used AI as a productivity and learning aid for the IaC and event-driven components. 

This reflects how I approach unfamiliar or less frequently used technologies in real-world environments: combining strong fundamentals with modern tooling to deliver reliable, production-ready solutions efficiently.


