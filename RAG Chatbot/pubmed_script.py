import os, time
from typing import List, Dict, Any, Optional
import pandas as pd
from Bio import Entrez
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

# ---------- PubMed fetch ----------

def _set_entrez_identity():
    Entrez.email = os.getenv("NCBI_EMAIL", "please-set-NCBI_EMAIL@example.com")
    api_key = os.getenv("NCBI_API_KEY")
    if api_key:
        Entrez.api_key = api_key

def _flatten_abstract(art: Dict[str, Any]) -> str:
    ab = art.get("Abstract")
    if not ab: return ""
    ab = ab.get("AbstractText", "")
    if isinstance(ab, str): return ab.strip()
    if isinstance(ab, list):
        parts = []
        for seg in ab:
            if isinstance(seg, str):
                parts.append(seg.strip())
            elif isinstance(seg, dict):
                txt = seg.get("#text") or seg.get("_") or ""
                label = seg.get("Label")
                parts.append(f"{label}: {txt}".strip() if label else txt.strip())
        return " ".join(p for p in parts if p)
    if isinstance(ab, dict):
        return (ab.get("#text") or ab.get("_") or "").strip()
    return ""

def _authors_to_str(art: Dict[str, Any]) -> str:
    names = []
    for a in art.get("AuthorList", []) or []:
        last = a.get("LastName", "") or ""
        fore = a.get("ForeName", "") or ""
        if not (last or fore):
            last = a.get("CollectiveName", "")
        nm = " ".join(x for x in [fore, last] if x).strip()
        if nm: names.append(nm)
    return ", ".join(names)

def fetch_pubmed_full(query: str, max_results: int = 1000, out_path: str = "pubmed_new.csv") -> str:
    """Fetch PubMed results → CSV, return absolute path."""
    _set_entrez_identity()

    h = Entrez.esearch(db="pubmed", term=query, retmax=max_results, sort="relevance")
    search = Entrez.read(h); h.close()
    ids: List[str] = search.get("IdList", [])

    if not ids:
        df = pd.DataFrame(columns=["PMID","Title","Authors","Year","Journal","Abstract"])
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        df.to_csv(out_path, index=False)
        return os.path.abspath(out_path)

    recs: List[Dict[str, Any]] = []
    for i in range(0, len(ids), 200):
        chunk = ids[i:i+200]
        f = Entrez.efetch(db="pubmed", id=",".join(chunk), rettype="medline", retmode="xml")
        parsed = Entrez.read(f); f.close()
        recs.extend(parsed.get("PubmedArticle", []))
        time.sleep(0.34)

    rows = []
    for r in recs:
        med = r.get("MedlineCitation", {})
        art = med.get("Article", {}) or {}
        jour = art.get("Journal", {}) or {}
        jtitle = (jour.get("Title") or "").strip()
        title  = (art.get("ArticleTitle") or "").strip()
        pubdate = (jour.get("JournalIssue", {}) or {}).get("PubDate", {})
        year = pubdate.get("Year") or (pubdate.get("MedlineDate") or "").split(" ")[0] or "N/A"
        pmid = str(med.get("PMID", "")).strip()
        authors = _authors_to_str(art)
        abstract = _flatten_abstract(art)
        rows.append({"PMID": pmid, "Title": title, "Authors": authors, "Year": str(year),
                     "Journal": jtitle, "Abstract": abstract})

    df = pd.DataFrame(rows).drop_duplicates(subset=["PMID"])
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df.to_csv(out_path, index=False)
    return os.path.abspath(out_path)

# ---------- Blob helpers + merge ----------

def download_existing_blob(conn_str: str, container: str, blob_name: str, local_path: str) -> Optional[str]:
    """Download existing blob if present; return local path or None."""
    bsc = BlobServiceClient.from_connection_string(conn_str)
    bc = bsc.get_blob_client(container=container, blob=blob_name)
    try:
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(bc.download_blob().readall())
        return os.path.abspath(local_path)
    except ResourceNotFoundError:
        return None

def merge_csvs(old_csv: Optional[str], new_csv: str, out_csv: str) -> str:
    """Union old+new, drop dups by PMID (fallback Title+Year), write merged CSV."""
    cols = ["PMID","Title","Authors","Year","Journal","Abstract"]
    def read(p):
        return pd.read_csv(p, dtype=str, keep_default_na=False) if p and os.path.exists(p) else pd.DataFrame(columns=cols)

    old_df, new_df = read(old_csv), read(new_csv)
    for c in cols:
        if c not in old_df.columns: old_df[c] = ""
        if c not in new_df.columns: new_df[c] = ""

    combined = pd.concat([old_df[cols], new_df[cols]], ignore_index=True)

    has_pmid = combined["PMID"].astype(str).str.len() > 0
    d1 = combined[has_pmid].drop_duplicates(subset=["PMID"], keep="last")
    d2 = combined[~has_pmid].drop_duplicates(subset=["Title","Year"], keep="last")
    merged = pd.concat([d1, d2], ignore_index=True)

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    merged.to_csv(out_csv, index=False)
    return os.path.abspath(out_csv)

def upload_to_blob(local_file_path: str, container_name: str, blob_name: str, connection_string: str) -> str:
    bsc = BlobServiceClient.from_connection_string(connection_string)
    cc = bsc.get_container_client(container_name)
    try: cc.create_container()
    except Exception: pass
    with open(local_file_path, "rb") as f:
        cc.upload_blob(name=blob_name, data=f, overwrite=True,
                       content_settings=ContentSettings(content_type="text/csv"))
    return cc.get_blob_client(blob_name).url
