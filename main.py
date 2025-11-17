import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, date, time as dt_time
from types import SimpleNamespace
from typing import Any, Dict, List, Literal, Optional, Tuple
from uuid import uuid4

import hashlib
import pytz
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.api_core import exceptions as gexc
from google.cloud import storage
from pydantic import BaseModel, Field, validator


GCS_BUCKET_NAME = "data_research"
GCS_PREFIX = "binance_alpha"

CORS_ALLOWED_ORIGINS = os.getenv("ALPHA_ALLOWED_ORIGINS", "*")
if CORS_ALLOWED_ORIGINS == "*":
    CORS_ALLOW_ORIGINS: List[str] = []
    CORS_ALLOW_ORIGIN_REGEX: Optional[str] = ".*"
else:
    CORS_ALLOW_ORIGINS = [
        origin.strip()
        for origin in CORS_ALLOWED_ORIGINS.split(",")
        if origin.strip()
    ]
    CORS_ALLOW_ORIGIN_REGEX = None


# -----------------------------------------------------------------------------
# Storage helpers
# -----------------------------------------------------------------------------

def _normalize_path(*parts: str) -> str:
    return "/".join([p.strip("/") for p in parts if p and p.strip("/")])


class FileDocumentStore:
    """Minimal JSON document store backed by Google Cloud Storage."""

    def __init__(self) -> None:
        self.bucket_name = GCS_BUCKET_NAME
        self.prefix = GCS_PREFIX
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def close(self) -> None:
        self.client.close()

    def _blob(self, collection: str) -> storage.Blob:
        object_name = _normalize_path(self.prefix, f"{collection}.json")
        return self.bucket.blob(object_name)

    async def read_all(self, collection: str) -> List[Dict[str, Any]]:
        blob = self._blob(collection)

        def _download() -> str:
            try:
                if not blob.exists():
                    return "[]"
                return blob.download_as_text()
            except gexc.NotFound:
                return "[]"

        raw = await asyncio.to_thread(_download)
        try:
            data = json.loads(raw)
            return data if isinstance(data, list) else []
        except json.JSONDecodeError:
            return []

    async def write_all(self, collection: str, items: List[Dict[str, Any]]) -> None:
        blob = self._blob(collection)
        payload = json.dumps(items, ensure_ascii=False, default=str)
        await asyncio.to_thread(blob.upload_from_string, payload, "application/json")

    async def append(self, collection: str, doc: Dict[str, Any]) -> None:
        items = await self.read_all(collection)
        items.append(doc)
        await self.write_all(collection, items)

    def new_id(self) -> str:
        return uuid4().hex


class GCSCursor:
    def __init__(self, store: FileDocumentStore, collection: str, query: Dict[str, Any]):
        self.store = store
        self.collection = collection
        self.query = query

    async def to_list(self, length: Optional[int] = None) -> List[Dict[str, Any]]:
        items = await self.store.read_all(self.collection)
        filtered = [
            item for item in items if GCSCollection.matches(item, self.query)
        ]
        if length is None:
            return filtered
        return filtered[:length]


class GCSCollection:
    def __init__(self, store: FileDocumentStore, name: str):
        self.store = store
        self.name = name

    @staticmethod
    def matches(doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        for key, expected in (query or {}).items():
            actual = doc.get(key)
            if isinstance(expected, re.Pattern):
                if not isinstance(actual, str) or not expected.search(actual):
                    return False
            else:
                if actual != expected:
                    return False
        return True

    def find(self, query: Optional[Dict[str, Any]] = None) -> GCSCursor:
        return GCSCursor(self.store, self.name, query or {})

    async def find_one(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        items = await self.store.read_all(self.name)
        for item in items:
            if self.matches(item, query):
                return item
        return None

    async def insert_one(self, doc: Dict[str, Any]) -> SimpleNamespace:
        item = dict(doc)
        item["_id"] = item.get("_id") or self.store.new_id()
        await self.store.append(self.name, item)
        return SimpleNamespace(inserted_id=item["_id"])

    async def update_one(
        self, query: Dict[str, Any], update: Dict[str, Any]
    ) -> SimpleNamespace:
        if "$set" not in update:
            raise ValueError("Only $set updates are supported")
        replacement = update["$set"]
        items = await self.store.read_all(self.name)
        modified = 0
        for idx, item in enumerate(items):
            if self.matches(item, query):
                updated = dict(item)
                updated.update(replacement)
                items[idx] = updated
                modified = 1
                break
        if modified:
            await self.store.write_all(self.name, items)
        return SimpleNamespace(modified_count=modified)

    async def delete_one(self, query: Dict[str, Any]) -> SimpleNamespace:
        items = await self.store.read_all(self.name)
        deleted = 0
        for idx, item in enumerate(items):
            if self.matches(item, query):
                items.pop(idx)
                deleted = 1
                break
        if deleted:
            await self.store.write_all(self.name, items)
        return SimpleNamespace(deleted_count=deleted)


class Database:
    store: Optional[FileDocumentStore] = None

    @classmethod
    def get_store(cls) -> FileDocumentStore:
        if cls.store is None:
            cls.store = FileDocumentStore()
        return cls.store

    @classmethod
    async def close(cls):
        if cls.store:
            cls.store.close()
            cls.store = None


def get_collection(name: str = "airdrops") -> GCSCollection:
    return GCSCollection(Database.get_store(), name)


def get_coin_collection() -> GCSCollection:
    return get_collection("coins")


def get_token_collection() -> GCSCollection:
    return get_collection("tokens")


def get_alpha_insight_collection() -> GCSCollection:
    return get_collection("alpha_insights")


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------


class AirdropBase(BaseModel):
    project: str = Field(..., min_length=1, max_length=100)
    alias: str = Field(..., min_length=1, max_length=100)
    points: Optional[float] = Field(None, ge=0)
    amount: Optional[float] = Field(None, ge=0)
    event_date: date
    event_time: Optional[dt_time] = Field(
        None, description="Event time (HH:MM or HH:MM:SS)"
    )
    timezone: Optional[str] = Field(
        None, description="e.g., Asia/Ho_Chi_Minh; defaults to UTC"
    )
    phase: Optional[str] = Field(None, max_length=100)
    x: Optional[str] = None
    raised: Optional[str] = None
    source_link: Optional[str] = None
    image_url: Optional[str] = None

    @validator("event_time", pre=True)
    def empty_time_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @validator("timezone", pre=True)
    def empty_timezone_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @validator("timezone")
    def validate_timezone(cls, v):
        if v is not None:
            if v not in pytz.all_timezones:
                raise ValueError("Invalid timezone")
        return v

    @validator("points", "amount", pre=True)
    def empty_numeric_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v


class AirdropCreate(AirdropBase):
    pass


class AirdropUpdate(BaseModel):
    project: Optional[str] = Field(None, min_length=1, max_length=100)
    alias: Optional[str] = Field(None, min_length=1, max_length=100)
    points: Optional[float] = Field(None, ge=0)
    amount: Optional[float] = Field(None, ge=0)
    event_date: Optional[date] = None
    event_time: Optional[dt_time] = None
    timezone: Optional[str] = None
    phase: Optional[str] = Field(None, max_length=100)
    x: Optional[str] = None
    raised: Optional[str] = None
    source_link: Optional[str] = None
    image_url: Optional[str] = None

    @validator("event_time", pre=True)
    def empty_time_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @validator("timezone", pre=True)
    def empty_timezone_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @validator("timezone")
    def validate_timezone(cls, v):
        if v is not None and v not in pytz.all_timezones:
            raise ValueError("Invalid timezone")
        return v


class AirdropResponse(BaseModel):
    id: str
    project: str
    alias: str
    points: Optional[float]
    amount: Optional[float]
    event_date: Optional[str]
    event_time: Optional[str]
    timezone: Optional[str]
    phase: Optional[str]
    x: Optional[str]
    raised: Optional[str]
    source_link: Optional[str]
    image_url: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]
    deleted: bool = False


class CoinData(BaseModel):
    coin_id: str
    time: datetime
    price: float


class CoinDataResponse(CoinData):
    id: str


class TokenBase(BaseModel):
    name: str
    apiUrl: str
    staggerDelay: int
    multiplier: float


class TokenCreate(TokenBase):
    pass


class TokenUpdate(BaseModel):
    name: Optional[str] = None
    apiUrl: Optional[str] = None
    staggerDelay: Optional[int] = None
    multiplier: Optional[float] = None


class TokenResponse(TokenBase):
    id: str


class AlphaInsightBase(BaseModel):
    title: str
    category: str
    token: str
    platform: str
    raised: str
    description: str
    date: str
    imageUrl: Optional[str] = None
    url: Optional[str] = None


class AlphaInsightCreate(AlphaInsightBase):
    pass


class AlphaInsightUpdate(BaseModel):
    title: Optional[str] = None
    category: Optional[str] = None
    token: Optional[str] = None
    platform: Optional[str] = None
    raised: Optional[str] = None
    description: Optional[str] = None
    date: Optional[str] = None
    imageUrl: Optional[str] = None
    url: Optional[str] = None


class AlphaInsightResponse(AlphaInsightBase):
    id: str


class AccountBase(BaseModel):
    name: str
    balance: float
    alphaPoints: float


class AccountCreate(AccountBase):
    pass


class AccountUpdate(BaseModel):
    name: Optional[str] = None
    balance: Optional[float] = None
    alphaPoints: Optional[float] = None


class AccountResponse(AccountBase):
    id: str


class AirdropItem(BaseModel):
    token: str
    amount: float
    price: float
    value: float


class TransactionBase(BaseModel):
    accountId: str
    date: str
    alphaPoints: float
    initialBalance: float
    finalBalance: float
    tradeFee: float
    note: Optional[str] = None
    airdrops: List[AirdropItem] = Field(default_factory=list)
    airdropToken: Optional[str] = None
    airdropAmount: Optional[float] = None
    airdropTokenPrice: Optional[float] = None
    pnl: float
    alphaReward: float
    airdropValue: Optional[float] = None
    totalClaim: float


class TransactionCreate(TransactionBase):
    pass


class TransactionUpdate(BaseModel):
    accountId: Optional[str] = None
    date: Optional[str] = None
    alphaPoints: Optional[float] = None
    initialBalance: Optional[float] = None
    finalBalance: Optional[float] = None
    tradeFee: Optional[float] = None
    note: Optional[str] = None
    airdrops: Optional[List[AirdropItem]] = None
    airdropToken: Optional[str] = None
    airdropAmount: Optional[float] = None
    airdropTokenPrice: Optional[float] = None
    pnl: Optional[float] = None
    alphaReward: Optional[float] = None
    airdropValue: Optional[float] = None
    totalClaim: Optional[float] = None


class TransactionResponse(TransactionBase):
    id: str


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------


def _ensure_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    raise ValueError("Invalid date value")


def _ensure_time(value: Any) -> Optional[dt_time]:
    if value is None:
        return None
    if isinstance(value, dt_time):
        return value.replace(microsecond=0)
    if isinstance(value, datetime):
        return value.time().replace(microsecond=0)
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(candidate, fmt).time()
            except ValueError:
                continue
        raise ValueError("Invalid time format, expected HH:MM or HH:MM:SS")
    raise ValueError("Invalid time value")


def compute_time_fields(
    event_date: Any, event_time: Any, timezone: Optional[str]
) -> Tuple[str, Optional[str], str]:
    date_obj = _ensure_date(event_date)
    time_obj = _ensure_time(event_time)
    time_for_dt = time_obj or dt_time(0, 0)
    tz_name = timezone or "UTC"
    tz = pytz.timezone(tz_name)
    dt_local = tz.localize(datetime.combine(date_obj, time_for_dt))
    return (
        date_obj.isoformat(),
        time_obj.isoformat() if time_obj else None,
        dt_local.isoformat(),
    )


def _coerce_datetime(
    time_iso: Optional[str], event_date: Any, event_time: Any, timezone: Optional[str]
) -> Tuple[datetime, pytz.BaseTzInfo]:
    tz_name = timezone or "UTC"
    tz = pytz.timezone(tz_name)

    if event_date is not None:
        try:
            date_obj = _ensure_date(event_date)
            time_obj = _ensure_time(event_time) or dt_time(0, 0)
            dt_local = tz.localize(datetime.combine(date_obj, time_obj))
            return dt_local, tz
        except Exception:
            pass

    if time_iso:
        dt = datetime.fromisoformat(time_iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(tz)
        return dt, tz

    raise ValueError("Missing event schedule information")


def generate_etag(data: Any) -> str:
    json_str = json.dumps(data, sort_keys=True, default=str)
    return f'W/"{hashlib.md5(json_str.encode()).hexdigest()}"'


def is_today(time_iso: Optional[str], event_date: Any, event_time: Any, timezone: Optional[str]) -> bool:
    try:
        dt, tz = _coerce_datetime(time_iso, event_date, event_time, timezone)
        now_tz = datetime.now(tz)
        return dt.date() == now_tz.date()
    except Exception:
        return False


def is_upcoming(time_iso: Optional[str], event_date: Any, event_time: Any, timezone: Optional[str]) -> bool:
    try:
        dt, tz = _coerce_datetime(time_iso, event_date, event_time, timezone)
        now_tz = datetime.now(tz)
        return dt.date() > now_tz.date()
    except Exception:
        return False


def filter_by_range(items: List[Dict[str, Any]], range_type: str) -> List[Dict[str, Any]]:
    if range_type == "all":
        return items

    filtered = []
    for item in items:
        time_iso = item.get("time_iso")
        event_date = item.get("event_date")
        event_time = item.get("event_time")
        timezone = item.get("timezone")

        if range_type == "today" and is_today(time_iso, event_date, event_time, timezone):
            filtered.append(item)
        elif range_type == "upcoming" and is_upcoming(time_iso, event_date, event_time, timezone):
            filtered.append(item)
    return filtered


def serialize_airdrop(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def serialize_coin(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def serialize_token(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def serialize_alpha_insight(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def serialize_account(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def serialize_transaction(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    return doc


def _validate_id(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HTTPException(status_code=400, detail="Invalid ID format")
    return value.strip()


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def apply_schedule_fields(
    data: Dict[str, Any], existing: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    merged = dict(data)

    def resolved_value(key: str):
        if key in data:
            return data[key]
        if existing:
            return existing.get(key)
        return None

    event_date_value = resolved_value("event_date")
    event_time_value = resolved_value("event_time")
    timezone_value = resolved_value("timezone")

    if event_date_value is None and existing and existing.get("time_iso"):
        try:
            derived_dt = datetime.fromisoformat(
                existing["time_iso"].replace("Z", "+00:00")
            )
            event_date_value = derived_dt.date()
            if event_time_value is None:
                event_time_value = derived_dt.time()
        except Exception:
            pass

    if event_date_value is None:
        raise HTTPException(status_code=400, detail="event_date is required")

    try:
        normalized_date, normalized_time, time_iso = compute_time_fields(
            event_date_value, event_time_value, timezone_value
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    merged["event_date"] = normalized_date
    merged["event_time"] = normalized_time
    merged["time_iso"] = time_iso
    if "timezone" not in merged and (timezone_value is not None or existing):
        merged["timezone"] = timezone_value

    return merged


# -----------------------------------------------------------------------------
# FastAPI setup
# -----------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await Database.close()


app = FastAPI(
    title="Binance Alpha GCS Backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_origin_regex=CORS_ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["ETag", "Last-Modified", "Content-Type"],
    max_age=3600,
)


@app.middleware("http")
async def ensure_cors_headers(request: Request, call_next):
    """Guarantee CORS headers even when exceptions bubble up."""
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": request.headers.get("origin", "*"),
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return Response(status_code=204, headers=headers)

    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = request.headers.get("origin", "*")
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=400, content={"error": "invalid_payload", "detail": str(exc)})


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "detail": str(exc)},
    )


def verify_admin():
    return "admin"


# -----------------------------------------------------------------------------
# Public routes
# -----------------------------------------------------------------------------

public_router = APIRouter(tags=["Public"])


@public_router.get("/api/airdrops")
async def get_airdrops(
    request: Request,
    response: Response,
    range: Literal["today", "upcoming", "all"] = Query("all"),
):
    collection = get_collection()
    cursor = collection.find({"deleted": False})
    items = await cursor.to_list(length=1000)
    items = [serialize_airdrop(item) for item in items]
    filtered = filter_by_range(items, range)

    def _sort_value(item: Dict[str, Any]) -> datetime:
        value = item.get("time_iso")
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return datetime.min
        return datetime.min

    filtered.sort(key=_sort_value, reverse=True)

    etag = generate_etag(filtered)

    def _updated_at_value(item):
        value = item.get("updated_at")
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return datetime.min
        return datetime.min

    last_modified_dt = None
    if filtered:
        latest = max(filtered, key=_updated_at_value)
        candidate = _updated_at_value(latest)
        if candidate != datetime.min:
            last_modified_dt = candidate

    if request.headers.get("if-none-match") == etag:
        response.status_code = 304
        return Response(
            status_code=304,
            headers={
                "ETag": etag,
                "Cache-Control": "public, max-age=5, must-revalidate, stale-while-revalidate=30",
            },
        )

    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "public, max-age=5, must-revalidate, stale-while-revalidate=30"
    if last_modified_dt:
        response.headers["Last-Modified"] = last_modified_dt.strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
    return {"items": filtered, "etag": etag}


@public_router.post("/api/coins", status_code=201)
async def save_coin_data(coin_data: CoinData):
    collection = get_coin_collection()
    doc = coin_data.dict()
    now = _now_iso()
    doc["created_at"] = now
    await collection.insert_one(doc)
    return {"status": "success", "data": doc}


@public_router.get("/api/coins/{coin_id}", response_model=List[CoinDataResponse])
async def get_coin_data(coin_id: str):
    collection = get_coin_collection()
    cursor = collection.find({"coin_id": coin_id})
    items = await cursor.to_list(length=1000)
    return [serialize_coin(item) for item in items]


# -----------------------------------------------------------------------------
# Admin / CRUD routes
# -----------------------------------------------------------------------------

admin_router = APIRouter(tags=["Admin"])


@admin_router.post("/api/airdrops", status_code=201, response_model=AirdropResponse)
async def create_airdrop(airdrop: AirdropCreate, _: str = Depends(verify_admin)):
    collection = get_collection()
    now = _now_iso()
    doc = apply_schedule_fields(airdrop.dict())
    project_name = doc.get("project")
    if not project_name:
        raise HTTPException(status_code=400, detail="Project name is required")

    project_regex = re.compile(f"^{re.escape(project_name.strip())}$", re.IGNORECASE)
    existing = await collection.find_one({"project": project_regex})

    if existing:
        update_data = doc
        update_data["updated_at"] = now
        await collection.update_one({"_id": existing["_id"]}, {"$set": update_data})
        updated_doc = await collection.find_one({"_id": existing["_id"]})
        return serialize_airdrop(updated_doc)
    else:
        doc.update({"created_at": now, "updated_at": now, "deleted": False})
        result = await collection.insert_one(doc)
        created_doc = await collection.find_one({"_id": result.inserted_id})
        return serialize_airdrop(created_doc)


@admin_router.put("/api/airdrops/{id}", response_model=AirdropResponse)
async def update_airdrop(id: str, airdrop: AirdropCreate, _: str = Depends(verify_admin)):
    collection = get_collection()
    object_id = _validate_id(id)
    existing = await collection.find_one({"_id": object_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Airdrop not found")

    update_data = apply_schedule_fields(airdrop.dict(exclude_unset=True), existing)
    update_data["updated_at"] = _now_iso()
    await collection.update_one({"_id": object_id}, {"$set": update_data})
    updated = await collection.find_one({"_id": object_id})
    return serialize_airdrop(updated)


@admin_router.delete("/api/airdrops/{id}", status_code=204)
async def delete_airdrop(id: str, _: str = Depends(verify_admin)):
    collection = get_collection()
    object_id = _validate_id(id)
    existing = await collection.find_one({"_id": object_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Airdrop not found")

    result = await collection.delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Airdrop not found")
    return Response(status_code=204)


@admin_router.get("/api/admin/airdrops", response_model=List[AirdropResponse])
async def get_all_airdrops(_: str = Depends(verify_admin)):
    collection = get_collection()
    cursor = collection.find({})
    items = await cursor.to_list(length=1000)
    return [serialize_airdrop(item) for item in items]


@admin_router.get("/api/admin/airdrops/deleted", response_model=List[AirdropResponse])
async def get_deleted_airdrops(_: str = Depends(verify_admin)):
    collection = get_collection()
    cursor = collection.find({"deleted": True})
    items = await cursor.to_list(length=1000)
    return [serialize_airdrop(item) for item in items]


# -----------------------------------------------------------------------------
# Tokens
# -----------------------------------------------------------------------------

token_router = APIRouter(tags=["Tokens"])


@token_router.post("/api/tokens", status_code=201, response_model=TokenResponse)
async def create_token(token: TokenCreate):
    collection = get_token_collection()
    doc = token.dict()
    result = await collection.insert_one(doc)
    created = await collection.find_one({"_id": result.inserted_id})
    return serialize_token(created)


@token_router.get("/api/tokens", response_model=List[TokenResponse])
async def get_all_tokens():
    collection = get_token_collection()
    cursor = collection.find({})
    items = await cursor.to_list(length=1000)
    return [serialize_token(item) for item in items]


@token_router.put("/api/tokens/{id}", response_model=TokenResponse)
async def update_token(id: str, token: TokenUpdate):
    collection = get_token_collection()
    object_id = _validate_id(id)
    existing = await collection.find_one({"_id": object_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Token not found")

    update_data = {k: v for k, v in token.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    await collection.update_one({"_id": object_id}, {"$set": update_data})
    updated = await collection.find_one({"_id": object_id})
    return serialize_token(updated)


@token_router.delete("/api/tokens/{id}", status_code=204)
async def delete_token(id: str):
    collection = get_token_collection()
    object_id = _validate_id(id)
    result = await collection.delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Token not found")
    return Response(status_code=204)


# -----------------------------------------------------------------------------
# Alpha insights
# -----------------------------------------------------------------------------

alpha_router = APIRouter(tags=["Alpha Insights"])


@alpha_router.post("/api/alpha-insights", status_code=201, response_model=AlphaInsightResponse)
async def create_alpha_insight(insight: AlphaInsightCreate):
    collection = get_alpha_insight_collection()
    doc = insight.dict()
    result = await collection.insert_one(doc)
    created = await collection.find_one({"_id": result.inserted_id})
    return serialize_alpha_insight(created)


@alpha_router.get("/api/alpha-insights", response_model=List[AlphaInsightResponse])
async def get_all_alpha_insights():
    collection = get_alpha_insight_collection()
    cursor = collection.find({})
    items = await cursor.to_list(length=1000)
    return [serialize_alpha_insight(item) for item in items]


@alpha_router.put("/api/alpha-insights/{id}", response_model=AlphaInsightResponse)
async def update_alpha_insight(id: str, insight: AlphaInsightUpdate):
    collection = get_alpha_insight_collection()
    object_id = _validate_id(id)
    existing = await collection.find_one({"_id": object_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Alpha insight not found")

    update_data = {k: v for k, v in insight.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    await collection.update_one({"_id": object_id}, {"$set": update_data})
    updated = await collection.find_one({"_id": object_id})
    return serialize_alpha_insight(updated)


@alpha_router.delete("/api/alpha-insights/{id}", status_code=204)
async def delete_alpha_insight(id: str):
    collection = get_alpha_insight_collection()
    object_id = _validate_id(id)
    result = await collection.delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Alpha insight not found")
    return Response(status_code=204)


# -----------------------------------------------------------------------------
# Accounts
# -----------------------------------------------------------------------------

accounts_router = APIRouter(tags=["Accounts"])


@accounts_router.get("/api/accounts", response_model=List[AccountResponse])
async def get_accounts():
    collection = get_collection("accounts")
    cursor = collection.find({})
    items = await cursor.to_list(length=1000)
    return [serialize_account(item) for item in items]


@accounts_router.post("/api/accounts", status_code=201, response_model=AccountResponse)
async def create_account(account: AccountCreate):
    collection = get_collection("accounts")
    doc = account.dict()
    result = await collection.insert_one(doc)
    created = await collection.find_one({"_id": result.inserted_id})
    return serialize_account(created)


@accounts_router.put("/api/accounts/{id}", response_model=AccountResponse)
async def update_account(id: str, account: AccountUpdate):
    collection = get_collection("accounts")
    object_id = _validate_id(id)
    update_data = {k: v for k, v in account.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    await collection.update_one({"_id": object_id}, {"$set": update_data})
    updated = await collection.find_one({"_id": object_id})
    if not updated:
        raise HTTPException(status_code=404, detail="Account not found")
    return serialize_account(updated)


@accounts_router.delete("/api/accounts/{id}", status_code=204)
async def delete_account(id: str):
    collection = get_collection("accounts")
    object_id = _validate_id(id)
    result = await collection.delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Account not found")
    return Response(status_code=204)


# -----------------------------------------------------------------------------
# Transactions
# -----------------------------------------------------------------------------

transactions_router = APIRouter(tags=["Transactions"])


@transactions_router.get("/api/transactions", response_model=List[TransactionResponse])
async def get_transactions():
    collection = get_collection("transactions")
    cursor = collection.find({})
    items = await cursor.to_list(length=1000)
    return [serialize_transaction(item) for item in items]


@transactions_router.post("/api/transactions", status_code=201, response_model=TransactionResponse)
async def create_transaction(transaction: TransactionCreate):
    transactions_collection = get_collection("transactions")
    doc = transaction.dict()
    result = await transactions_collection.insert_one(doc)
    created = await transactions_collection.find_one({"_id": result.inserted_id})

    accounts_collection = get_collection("accounts")
    await accounts_collection.update_one(
        {"_id": transaction.accountId},
        {"$set": {"balance": transaction.finalBalance, "alphaPoints": transaction.alphaPoints}},
    )
    return serialize_transaction(created)


@transactions_router.put("/api/transactions/{id}", response_model=TransactionResponse)
async def update_transaction(id: str, transaction: TransactionUpdate):
    transactions_collection = get_collection("transactions")
    accounts_collection = get_collection("accounts")
    object_id = _validate_id(id)
    existing = await transactions_collection.find_one({"_id": object_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Transaction not found")

    update_data = {k: v for k, v in transaction.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")

    await transactions_collection.update_one({"_id": object_id}, {"$set": update_data})
    updated = await transactions_collection.find_one({"_id": object_id})

    if "finalBalance" in update_data or "alphaPoints" in update_data:
        account_updates = {}
        if "finalBalance" in update_data:
            account_updates["balance"] = update_data["finalBalance"]
        if "alphaPoints" in update_data:
            account_updates["alphaPoints"] = update_data["alphaPoints"]
        if account_updates:
            await accounts_collection.update_one(
                {"_id": existing["accountId"]},
                {"$set": account_updates},
            )

    return serialize_transaction(updated)


@transactions_router.delete("/api/transactions/{id}", status_code=204)
async def delete_transaction(id: str):
    collection = get_collection("transactions")
    object_id = _validate_id(id)
    result = await collection.delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return Response(status_code=204)


# -----------------------------------------------------------------------------
# App routes
# -----------------------------------------------------------------------------

app.include_router(public_router)
app.include_router(admin_router)
app.include_router(token_router)
app.include_router(alpha_router)
app.include_router(accounts_router)
app.include_router(transactions_router)


@app.get("/")
async def root():
    return {
        "message": "Airdrop Management API (GCS)",
        "version": "1.0.0",
        "endpoints": {
            "public": "/api/airdrops?range=today|upcoming|all",
            "admin": "/api/admin/airdrops",
        },
    }


@app.get("/health")
async def health():
    store = Database.get_store()
    return {"status": "ok", "bucket": store.bucket_name, "prefix": store.prefix}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
