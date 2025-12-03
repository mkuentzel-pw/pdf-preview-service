import os
import io
from typing import Optional

import boto3
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from pdf2image import convert_from_bytes

# --- ENV VARS ---

SPACES_KEY = os.environ.get("SPACES_KEY")
SPACES_SECRET = os.environ.get("SPACES_SECRET")
SPACES_ENDPOINT = os.environ.get("SPACES_ENDPOINT")  # e.g. "nyc3.digitaloceanspaces.com"
SPACES_REGION = os.environ.get("SPACES_REGION", "nyc3")
SPACES_BUCKET = os.environ.get("SPACES_BUCKET")

# Optional simple API key for this service (recommended)
API_KEY = os.environ.get("API_KEY")  # e.g. set in DO as a random long string

if not all([SPACES_KEY, SPACES_SECRET, SPACES_ENDPOINT, SPACES_BUCKET]):
    # Fail fast if misconfigured
    raise RuntimeError("Missing required Spaces environment variables")

# --- SPACES CLIENT ---

session = boto3.session.Session()
s3 = session.client(
    "s3",
    region_name=SPACES_REGION,
    endpoint_url=f"https://{SPACES_ENDPOINT}",
    aws_access_key_id=SPACES_KEY,
    aws_secret_access_key=SPACES_SECRET,
)

# --- APP ---

app = FastAPI()


class PdfToJpgRequest(BaseModel):
    pdfKey: str
    outputKeyPrefix: Optional[str] = None  # optional override (still forced under thumbnails/)


def get_object_bytes(key: str) -> bytes:
    try:
        resp = s3.get_object(Bucket=SPACES_BUCKET, Key=key)
        return resp["Body"].read()
    except Exception as e:
        print(f"Error fetching {key} from Spaces: {e}")
        raise HTTPException(status_code=404, detail="PDF not found")


def put_object_bytes(key: str, data: bytes, content_type: str = "image/jpeg") -> None:
    try:
        s3.put_object(
            Bucket=SPACES_BUCKET,
            Key=key,
            Body=data,
            ACL="private",
            ContentType=content_type,
        )
    except Exception as e:
        print(f"Error uploading {key} to Spaces: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload preview image")


def build_thumbnail_key(pdf_key: str, output_prefix: Optional[str]) -> str:
   
    if output_prefix:
        base_prefix = output_prefix.strip("/")
        if not base_prefix.startswith("thumbnails/"):
            base_prefix = f"thumbnails/{base_prefix}"
        base = base_prefix
    else:
        normalized = pdf_key.lstrip("/")
        if normalized.lower().endswith(".pdf"):
            normalized = normalized[:-4]
        base = f"thumbnails/{normalized}"

    return f"{base}-thumbnail.jpg"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/pdf-to-jpg")
def pdf_to_jpg(
    payload: PdfToJpgRequest,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
):
    # --- Simple API key check (optional but strongly recommended) ---
    if API_KEY:
        if not x_api_key or x_api_key != API_KEY:
            raise HTTPException(status_code=401, detail="Unauthorized")

    pdf_key = payload.pdfKey

    if not pdf_key:
        raise HTTPException(status_code=400, detail="pdfKey is required")

    # 1) Fetch PDF bytes from Spaces
    pdf_bytes = get_object_bytes(pdf_key)

    # 2) Convert first page of PDF -> image
    try:
        pages = convert_from_bytes(pdf_bytes, first_page=1, last_page=1)
    except Exception as e:
        print(f"Error converting PDF {pdf_key}: {e}")
        raise HTTPException(status_code=500, detail="Failed to convert PDF to image")

    if not pages:
        raise HTTPException(status_code=500, detail="No pages found in PDF")

    first_page = pages[0]

    # 3) Save as JPEG to a buffer
    img_buffer = io.BytesIO()
    first_page.save(img_buffer, format="JPEG", quality=80)
    img_buffer.seek(0)
    img_bytes = img_buffer.read()

    # 4) Decide output key (always under thumbnails/)
    jpg_key = build_thumbnail_key(pdf_key, payload.outputKeyPrefix)

    # 5) Upload JPEG to Spaces
    put_object_bytes(jpg_key, img_bytes, content_type="image/jpeg")

    return {
        "pdfKey": pdf_key,
        "jpgKey": jpg_key,
    }
