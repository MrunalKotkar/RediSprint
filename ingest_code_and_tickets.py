import os, csv, pathlib, hashlib
from dotenv import load_dotenv
from redisvl.index import SearchIndex
from redisvl.utils.vectorize import HFTextVectorizer
import tiktoken
from composio import Composio
import numpy as np

load_dotenv()

HF_EMBEDDING_MODEL = os.environ.get("HF_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
embedder = HFTextVectorizer(model=HF_EMBEDDING_MODEL)
tickets = SearchIndex.from_existing("tickets_idx", redis_url=os.environ["REDIS_URL"])
code = SearchIndex.from_existing("code_idx", redis_url=os.environ["REDIS_URL"])

SKIP_DIRS = {".git", ".hg", ".svn", "node_modules", "dist", "build", "target", ".venv", "__pycache__", ".txt"}
EXTS = {".py", ".ts", ".tsx", ".js", ".jsx", ".go", ".java", ".kt", ".rb", ".rs", ".php", ".cs", ".scala", ".md", ".yaml", ".yml", ".json", ".html"}
MAX_FILE_BYTES = 400_000


def chunk_by_tokens(text, max_tokens=500, overlap=60):
    enc = tiktoken.get_encoding("cl100k_base")
    toks = enc.encode(text)
    chunks = []
    i = 0
    while i < len(toks):
        window = toks[i:i + max_tokens]
        chunks.append(enc.decode(window))
        i += max_tokens - overlap
    return chunks


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def ingest_tickets():
    """
    Ingest historical tickets into Redis for semantic search context.
    Set TICKETS_CSV to a Jira export CSV path, or configure Composio Jira
    credentials to pull directly from Jira.
    """
    rows = []
    csv_path = os.getenv("TICKETS_CSV", "sample_data/past_tickets.csv")

    if csv_path and os.path.exists(csv_path):
        print(f"   Loading tickets from {csv_path}...")
        with open(csv_path) as f:
            csv_rows = list(csv.DictReader(f))
        for r in csv_rows:
            rows.append({
                "id": r.get("Issue key", r.get("id", sha1(r.get("Summary", "")))),
                "title": r.get("Summary", r.get("title", "")),
                "body": r.get("Description", r.get("body", "")),
                "issuetype": r.get("Issue Type", r.get("issuetype", "Task")),
                "component": r.get("component", ""),
                "assignee": r.get("Assignee", r.get("assignee", "")),
                "sprint": r.get("Sprint", os.getenv("SPRINT_NAME", "")),
            })
    else:
        # Fall back to Composio Jira integration
        print("   No CSV found — pulling tickets from Jira via Composio...")
        composio = Composio(api_key=os.environ["COMPOSIO_API_KEY"])
        user_id = os.environ["COMPOSIO_USER_ID"]
        tools = composio.tools.get(user_id=user_id, toolkits=["JIRA"])
        search_slug = next(
            (t["slug"] for t in tools if "SEARCH" in t["slug"] or "ISSUES_SEARCH" in t["slug"]),
            None
        )
        if not search_slug:
            print("   ❌ Could not find Jira search tool. Skipping ticket ingestion.")
            return

        res = composio.tools.execute(
            slug=search_slug,
            user_id=user_id,
            arguments={
                "jql": os.environ.get("TICKETS_JQL", "project = SPRINT ORDER BY created DESC"),
                "maxResults": 200
            }
        )
        for issue in res.get("issues", []):
            comps = [c.get("name", "") for c in (issue["fields"].get("components") or [])]
            rows.append({
                "id": issue["key"],
                "title": issue["fields"]["summary"],
                "body": (issue["fields"].get("description") or ""),
                "issuetype": issue["fields"]["issuetype"]["name"],
                "component": "|".join([c for c in comps if c]),
                "assignee": (issue["fields"].get("assignee") or {}).get("displayName", ""),
                "sprint": os.environ.get("SPRINT_NAME", ""),
            })

    if not rows:
        print("   ⚠️  No tickets to ingest.")
        return

    texts = [(r.get("title", "") + "\n" + r.get("body", "")).strip() for r in rows]
    embs = embedder.embed_many(texts)
    payloads = []
    for r, emb in zip(rows, embs):
        payloads.append({
            "id": r["id"],
            "title": r.get("title", ""),
            "body": r.get("body", ""),
            "issuetype": r.get("issuetype", "Task"),
            "component": r.get("component", ""),
            "assignee": r.get("assignee", ""),
            "sprint": r.get("sprint", ""),
            "embedding": np.array(emb, dtype=np.float32).tobytes(),
        })

    tickets.load(payloads, id_field="id")
    print(f"   ✅ Ingested {len(payloads)} tickets")


def ingest_code():
    """
    Ingest codebase files into Redis for semantic search context.
    Set CODE_DIR to the root of your project/codebase.
    """
    root = pathlib.Path(os.environ.get("CODE_DIR", ".")).resolve()
    repo = os.environ.get("REPO_NAME", root.name)
    print(f"   Scanning: {root} (repo: {repo})")

    payloads, text_batch, meta_batch = [], [], []
    batch_limit = 128

    for p in root.rglob("*"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        if p.is_dir():
            continue
        if (p.suffix.lower() not in EXTS) or (p.stat().st_size > MAX_FILE_BYTES):
            continue

        try:
            body = p.read_text(errors="ignore")
        except Exception:
            continue

        for idx, chunk in enumerate(chunk_by_tokens(body)):
            text_batch.append(chunk)
            meta_batch.append((
                f"{repo}:{p.as_posix()}:{idx}",
                p.as_posix(),
                (p.suffix or "").lstrip(".").lower()
            ))

            if len(text_batch) >= batch_limit:
                embs = embedder.embed_many(text_batch)
                for (doc_id, path, lang), emb, chunk_text in zip(meta_batch, embs, text_batch):
                    payloads.append({
                        "id": doc_id,
                        "repo": repo,
                        "path": path,
                        "lang": lang,
                        "body": chunk_text,
                        "embedding": np.array(emb, dtype=np.float32).tobytes(),
                    })
                code.load(payloads, id_field="id")
                payloads, text_batch, meta_batch = [], [], []

    if text_batch:
        embs = embedder.embed_many(text_batch)
        for (doc_id, path, lang), emb, chunk_text in zip(meta_batch, embs, text_batch):
            payloads.append({
                "id": doc_id,
                "repo": repo,
                "path": path,
                "lang": lang,
                "body": chunk_text,
                "embedding": np.array(emb, dtype=np.float32).tobytes(),
            })
        if payloads:
            code.load(payloads, id_field="id")

    print(f"   ✅ Code ingestion complete")


if __name__ == "__main__":
    print("\n🚀 Starting ingestion...\n")

    print("📋 Ingesting historical tickets...")
    ingest_tickets()

    print("\n💻 Ingesting codebase...")
    ingest_code()

    print("\n✅ Ingestion complete! Redis indices are ready.")
