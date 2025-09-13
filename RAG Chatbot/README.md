# Cardiovascular PubMed Pipeline

This repository contains code to automatically fetch PubMed articles related to cardiovascular diseases, merge them with an existing dataset, and upload the cumulative dataset to Azure Blob Storage.

## Files
- `pubmed_dag.py`: Airflow DAG that schedules the pipeline.
- `pubmed_script.py`: Core functions for fetching PubMed data and uploading to Azure.
- `requirements.txt`: Python dependencies.

## Usage
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
