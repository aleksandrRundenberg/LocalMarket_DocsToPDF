from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


# =========================
# CONFIG (GitHub Secrets / env)
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()
GOOGLE_TEMPLATE_DOC_ID = os.getenv("GOOGLE_TEMPLATE_DOC_ID", "").strip()
GOOGLE_TOKEN_JSON = os.getenv("GOOGLE_TOKEN_JSON", "").strip()

# Optional: Notion API version (keep stable)
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28").strip()

# Output folder for GitHub Actions artifact upload
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "out_pdfs"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Optional: delete intermediate Google Docs after export (recommended in CI)
DELETE_INTERMEDIATE_DOCS = os.getenv("DELETE_INTERMEDIATE_DOCS", "true").lower() in ("1", "true", "yes")


# =========================
# UTILS
# =========================
def die(msg: str) -> None:
    raise SystemExit(f"ERROR: {msg}")


def slugify(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\- ]+", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s)
    return s[:120] if s else "document"


def rich_plain_text(rich: List[Dict[str, Any]]) -> str:
    return "".join(x.get("plain_text", "") for x in (rich or []))


def notion_prop_to_str(prop: Dict[str, Any]) -> str:
    """Convert a Notion property object to a printable string (common types)."""
    t = prop.get("type")
    if not t:
        return ""

    v = prop.get(t)

    if t == "title":
        return rich_plain_text(v)
    if t == "rich_text":
        return rich_plain_text(v)
    if t == "number":
        return "" if v is None else str(v)
    if t == "checkbox":
        return "TRUE" if v else "FALSE"
    if t == "select":
        return v["name"] if v else ""
    if t == "multi_select":
        return ", ".join(x["name"] for x in (v or []))
    if t == "date":
        return v.get("start", "") if v else ""
    if t in ("email", "phone_number", "url"):
        return v or ""
    if t == "people":
        return ", ".join((p.get("name") or "") for p in (v or []))
    if t == "relation":
        return ", ".join(r.get("id", "") for r in (v or []))
    if t == "formula":
        ft = (v or {}).get("type")
        return "" if not ft else str((v or {}).get(ft) or "")
    if t == "status":
        return v["name"] if v else ""

    return str(v) if v is not None else ""


# =========================
# NOTION (raw HTTP, robust in CI)
# =========================
def fetch_all_notion_rows(database_id: str) -> List[Dict[str, Any]]:
    if not NOTION_TOKEN:
        die("NOTION_TOKEN is missing (set GitHub secret NOTION_TOKEN).")
    if not database_id:
        die("NOTION_DATABASE_ID is missing (set GitHub secret NOTION_DATABASE_ID).")

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

    results: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    with httpx.Client(timeout=60) as client:
        while True:
            payload: Dict[str, Any] = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor

            r = client.post(
                f"https://api.notion.com/v1/databases/{database_id}/query",
                headers=headers,
                json=payload,
            )

            if r.status_code >= 400:
                die(f"Notion API error {r.status_code}: {r.text}")

            data = r.json()
            results.extend(data.get("results", []))

            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")

    return results


def row_to_placeholder_map(page: Dict[str, Any]) -> Dict[str, str]:
    """
    Template placeholders must be: {{Column Name}}
    Column Name = Notion property name exactly.
    """
    props = page.get("properties", {})
    repl: Dict[str, str] = {}
    for col_name, prop_obj in props.items():
        repl[f"{{{{{col_name}}}}}"] = notion_prop_to_str(prop_obj)
    return repl


# =========================
# GOOGLE (OAuth refresh token from secret)
# =========================
def google_clients_from_token_json():
    if not GOOGLE_TOKEN_JSON:
        die("GOOGLE_TOKEN_JSON missing (set GitHub secret GOOGLE_TOKEN_JSON).")

    try:
        info = json.loads(GOOGLE_TOKEN_JSON)
    except json.JSONDecodeError:
        die("GOOGLE_TOKEN_JSON is not valid JSON. Paste the full google_token.json content into the secret.")

    creds = Credentials.from_authorized_user_info(info, SCOPES)

    # Refresh access token using refresh_token (headless)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            die("Google credentials invalid and cannot refresh. Re-generate google_token.json with refresh_token.")

    docs = build("docs", "v1", credentials=creds)
    drive = build("drive", "v3", credentials=creds)
    return docs, drive


def copy_template_doc(drive, template_doc_id: str, new_title: str) -> str:
    if not template_doc_id:
        die("GOOGLE_TEMPLATE_DOC_ID missing (set GitHub secret GOOGLE_TEMPLATE_DOC_ID).")

    copied = drive.files().copy(
        fileId=template_doc_id,
        body={"name": new_title, "mimeType": "application/vnd.google-apps.document"},
    ).execute()
    return copied["id"]


def replace_placeholders(docs, document_id: str, repl: Dict[str, str]) -> None:
    # One batchUpdate call with many replaceAllText requests
    requests = [
        {
            "replaceAllText": {
                "containsText": {"text": placeholder, "matchCase": True},
                "replaceText": value,
            }
        }
        for placeholder, value in repl.items()
    ]

    docs.documents().batchUpdate(
        documentId=document_id,
        body={"requests": requests},
    ).execute()


def export_pdf(drive, document_id: str, out_path: Path) -> None:
    req = drive.files().export_media(fileId=document_id, mimeType="application/pdf")
    data = req.execute()
    out_path.write_bytes(data)


# =========================
# MAIN
# =========================
def main() -> None:
    docs, drive = google_clients_from_token_json()

    rows = fetch_all_notion_rows(NOTION_DATABASE_ID)
    if not rows:
        die("No rows found in Notion database.")

    print(f"Found {len(rows)} rows. Output: {OUTPUT_DIR.resolve()}")

    for i, page in enumerate(rows, start=1):
        repl = row_to_placeholder_map(page)

        # filename based on likely columns
        preferred_cols = ["Invoice #", "Invoice", "Name", "Title", "ID"]
        base_name = None
        for col in preferred_cols:
            ph = f"{{{{{col}}}}}"
            if repl.get(ph, "").strip():
                base_name = repl[ph].strip()
                break
        if not base_name:
            base_name = f"row_{i}"

        safe_name = slugify(base_name)
        new_title = f"PDF_{safe_name}"

        new_doc_id = copy_template_doc(drive, GOOGLE_TEMPLATE_DOC_ID, new_title)
        replace_placeholders(docs, new_doc_id, repl)

        pdf_path = OUTPUT_DIR / f"{safe_name}.pdf"
        export_pdf(drive, new_doc_id, pdf_path)

        if DELETE_INTERMEDIATE_DOCS:
            drive.files().delete(fileId=new_doc_id).execute()

        print(f"[{i}/{len(rows)}] Saved: {pdf_path.name}")

    print("Done.")


if __name__ == "__main__":
    main()
