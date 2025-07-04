from datetime import datetime
import requests
import pandas as pd
import os


from urllib.parse import urljoin
from bs4 import BeautifulSoup  # para scrapear doc
from sentence_transformers import SentenceTransformer  # para vectorizar local
import chromadb
from requests import Session


# Disable telemetry to avoid PostHog errors
os.environ.setdefault("CHROMA_DISABLE_TELEMETRY", "1")

# Lazy initialization of Chroma collection
_chroma_client = None
_collection = None


def get_chroma_collection():
    global _chroma_client, _collection
    if _collection is None:
        _chroma_client = chromadb.PersistentClient(
            path=os.getenv("CHROMA_DIR", "./chromadb_data"),
        )
        _collection = _chroma_client.get_or_create_collection(
            name="sec_filings",
            metadata={"source": "sec_api", "hnsw:space": "cosine"},
        )
    return _collection


# Lazy initialization of embedding model
_embedding_model = None


def get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        # Use lightweight MiniLM for faster CPU embeddings
        _embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _embedding_model


# Headers to mimic a browser and avoid 403 from SEC
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)"
}

# Use a persistent Session to send more complete browser-like headers
session = Session()
session.headers.update(
    {
        "User-Agent": HTTP_HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Referer": "https://www.sec.gov/",
    }
)


# Default args del DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

# Constantes
API_KEY = "792ca02b4cbc590ff1b2e60bfd553839aa55f743134655cba5e98a8e1e997460"

# In the Airflow environment, this path is mounted from the host's ./airflow/output directory.
# For local script execution (if __name__ == "__main__"), it will create/use a local 'output' directory.
if "AIRFLOW_HOME" in os.environ:
    OUTPUT_DIR = "/opt/airflow/output"
else:
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


# Obtain Document Links from SEC API
def fetch_10K_docs() -> pd.DataFrame:
    """Fetch AAPL 10-K filings from SEC API and save as CSV"""
    print("[fetch_10K_docs] Starting fetch_10K_docs...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        url = "https://api.sec-api.io"

        headers = {"Authorization": API_KEY, "Content-Type": "application/json"}

        query = {
            "query": 'ticker:AAPL AND formType:"10-K"',
            "from": 0,
            "size": 5,
            "sort": [{"filedAt": {"order": "desc"}}],
        }

        response = requests.post(url, json=query, headers=headers)
        response.raise_for_status()

        data = response.json()
        filings = data.get("filings") or data.get("results") or []

        docs = [
            {
                "formType": f.get("formType"),
                "filedAt": f.get("filedAt"),
                "linkToFiling": f.get("linkToHtml") or f.get("linkToFilingDetails"),
                "companyName": f.get("companyName") or "Apple",
            }
            for f in filings
        ]

        df = pd.DataFrame(docs)
        print(f"[fetch_10K_docs] Fetched {len(df)} filings.")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df.to_csv(os.path.join(OUTPUT_DIR, "appl_10k.csv"), index=False)
        print("CSV generado en:", os.path.join(OUTPUT_DIR, "appl_10k.csv"))
        print(df.head())
        return df

    except Exception as e:
        print("Error al obtener datos de la SEC API:", str(e))
        raise


# Extract Text from HTML Link from Dataframe
def extract_text_from_html(html: str) -> str:
    """Extract main text from HTML, focusing on headings and paragraphs."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove scripts and styles
    for tag in soup(["script", "style"]):
        tag.decompose()

    # Extract all text and split into non-empty lines
    full_text = soup.get_text(separator="\n")
    lines = [line.strip() for line in full_text.splitlines() if line.strip()]
    return "\n".join(lines)


def get_full_filing_url(index_url: str) -> str:
    """ "given the index page URL, return the url of the actual HtML filing document"""
    try:
        resp = session.get(index_url)
    except Exception as e:
        print(
            f"[get_full_filing_url] Warning: 403 fetching index URL; using index URL directly"
        )
        return index_url
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", {"summary": "Document Format Files"})
    if table:
        for a in table.find_all("a", href=True):
            href = a["href"]
            # If SEC returns an inline XBRL 'ix?doc=' link, convert to the real HTML URL
            if href.startswith("/ix?doc="):
                # extract the path after 'doc='
                raw_path = href.split("ix?doc=")[1]
                return f"https://www.sec.gov{raw_path}"
            if href.lower().endswith(".htm") and "index" not in href.lower():
                return urljoin(index_url, href)
        return index_url
    # fallback: search all <a> tags
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # If SEC returns an inline XBRL 'ix?doc=' link, convert to the real HTML URL
        if href.startswith("/ix?doc="):
            # extract the path after 'doc='
            raw_path = href.split("ix?doc=")[1]
            return f"https://www.sec.gov{raw_path}"
        if href.lower().endswith(".htm") and "index" not in href.lower():
            return urljoin(index_url, href)
    return index_url


# Split into Chunks
def split_into_chunks(text: str, chunk_size: int = 500, overlap: int = 0) -> list[str]:
    """Split text into paragraph-aware chunks of approximately chunk_size characters with overlap."""
    # Break text into non-empty paragraphs
    paras = [p.strip() for p in text.split("\n") if p.strip()]
    chunks = []
    current = ""
    for para in paras:
        # If adding this paragraph stays within chunk_size, append
        if len(current) + len(para) + 1 <= chunk_size:
            current += para + " "
        else:
            # finalize current chunk
            chunks.append(current.strip())
            # start new chunk, carrying overlap from end of previous
            overlap_text = current[-overlap:]
            current = overlap_text + " " + para + " "
    # Add any remaining text as final chunk
    if current:
        chunks.append(current.strip())
    return chunks


def upsert_vectors(
    index_name: str,
    vectors: list[tuple[str, list[float]]],
    documents: list[str],
    metadata: dict,
) -> None:
    """Insert Vectors into your ChromaDB
    Vectors: list of tuples (id, vector)
    metadata: dict of metadata for each vector"""
    col = get_chroma_collection()
    ids, vecs = zip(*vectors)
    metas = [metadata for _ in ids]
    col.add(embeddings=list(vecs), ids=list(ids), documents=documents, metadatas=metas)


def process_and_index(
    df: pd.DataFrame, chunk_size: int = 1000, overlap: int = 200
) -> None:
    """Extracts texct from each filling URL, Splits into Chunks, generate embeddings, and upsert into vector DB."""
    print(f"[process_and_index] Starting indexing of {len(df)} filings...", flush=True)
    for idx, row in df.iterrows():
        index_url = row.get("linkToFiling")
        content_url = get_full_filing_url(index_url)
        print(f"[process_and_index] Doc {idx+1}/{len(df)}: processing {content_url}")
        try:
            resp = session.get(content_url)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            print(
                f"[process_and_index] Warning: could not fetch content URL, falling back to index_url: {e}"
            )
            # Try index_url without parsing
            html = session.get(index_url).text
        text = extract_text_from_html(html)
        chunks = split_into_chunks(text, chunk_size, overlap)
        print(
            f"[process_and_index] {len(chunks)} chunks generated for document {idx+1}",
            flush=True,
        )
        if not chunks:
            print(
                f"[process_and_index] Warning: no chunks generated for document {idx+1}, skipping."
            )
            continue
        # Batch embedding and upsert to avoid long single operations
        model = get_embedding_model()
        batch_size = 32
        total = 0
        metadata = {"ticker": row.get("companyName"), "filedAt": row.get("filedAt")}
        for start in range(0, len(chunks), batch_size):
            end = start + batch_size
            batch_chunks = chunks[start:end]
            batch_embs = model.encode(
                batch_chunks, batch_size=batch_size, show_progress_bar=False
            )
            batch_vectors = [
                (f"{row.get('companyName')}_{start + j}", emb.tolist())
                for j, emb in enumerate(batch_embs)
            ]
            upsert_vectors("sec_filings", batch_vectors, batch_chunks, metadata)
            total += len(batch_vectors)
            print(
                f"[process_and_index] Upserted batch {start//batch_size+1} ({len(batch_vectors)} vectors)",
                flush=True,
            )
        print(
            f"[process_and_index] Upserted total {total} vectors for document {idx+1}",
            flush=True,
        )
    print("[process_and_index] Completed indexing all documents.", flush=True)


# Level 1 Testing
if __name__ == "__main__":
    df = fetch_10K_docs()
    process_and_index(df)
