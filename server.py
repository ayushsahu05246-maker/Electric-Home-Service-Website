from fastapi import FastAPI, APIRouter, HTTPException, Request, Response, Depends
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
import os
import logging
import asyncio
import uuid
import json
import hmac
import hashlib
import httpx
import resend
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Literal
from datetime import datetime, timezone, timedelta

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

mongo_url = os.environ.get('MONGO_URL', '')
db_name = os.environ.get('DB_NAME', 'electric_service')
client = (
    AsyncIOMotorClient(
        mongo_url,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        socketTimeoutMS=10000,
        tls=True,
        tlsAllowInvalidCertificates=False,
        retryWrites=True,
        w="majority",
    )
    if mongo_url
    else None
)
db = client[db_name] if client else None

ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'ayushsahu05246@gmail.com').lower()
RESEND_API_KEY = os.environ.get('RESEND_API_KEY', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'onboarding@resend.dev')
WHATSAPP_NUMBER = os.environ.get('WHATSAPP_NUMBER', '9454386338')
ADMIN_PORTAL_PIN = os.environ.get('ADMIN_PORTAL_PIN', '1234')
ADMIN_SESSION_SECRET = os.environ.get('ADMIN_SESSION_SECRET', ADMIN_PORTAL_PIN)
COOKIE_SECURE = os.environ.get('RENDER', '') == 'true' or os.environ.get('COOKIE_SECURE', '').lower() == 'true'
IS_PRODUCTION = os.environ.get('RENDER', '').lower() == 'true'
ADMIN_SESSION_MAX_AGE = 7 * 24 * 60 * 60
BOOKINGS_FILE = ROOT_DIR / "bookings.local.json"
MONGO_SAVE_RETRIES = 3

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY

app = FastAPI()
api_router = APIRouter(prefix='/api')


def _load_local_bookings() -> list[dict]:
    if not BOOKINGS_FILE.exists():
        return []
    try:
        with BOOKINGS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_local_bookings(items: list[dict]) -> None:
    with BOOKINGS_FILE.open("w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)


def _merge_bookings(*sources: list[dict]) -> list[dict]:
    """Merge booking lists; later sources override earlier ones for the same booking_id."""
    by_id: dict[str, dict] = {}
    for source in sources:
        for item in source:
            booking_id = item.get("booking_id")
            if booking_id:
                by_id[booking_id] = item
    merged = list(by_id.values())
    merged.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return merged


def _stats_from_items(items: list[dict]) -> dict:
    return {
        'total': len(items),
        'pending': sum(1 for x in items if x.get("status") == "pending"),
        'in_progress': sum(1 for x in items if x.get("status") == "in_progress"),
        'completed': sum(1 for x in items if x.get("status") == "completed"),
    }


async def _get_mongo_bookings() -> list[dict]:
    if db is None:
        return []
    try:
        return await db.bookings.find({}, {'_id': 0}).sort('created_at', -1).to_list(5000)
    except PyMongoError as e:
        logger.error("Could not load bookings from MongoDB: %s", e)
        return []


async def _save_booking_to_mongo(doc: dict) -> bool:
    """Persist booking permanently in MongoDB with retries + upsert."""
    if db is None:
        return False
    payload = doc.copy()
    payload.pop('_id', None)
    for attempt in range(1, MONGO_SAVE_RETRIES + 1):
        try:
            await db.bookings.replace_one(
                {'booking_id': payload['booking_id']},
                payload,
                upsert=True,
            )
            return True
        except PyMongoError as e:
            logger.error("MongoDB save attempt %s/%s failed: %s", attempt, MONGO_SAVE_RETRIES, e)
            if attempt < MONGO_SAVE_RETRIES:
                await asyncio.sleep(1.5 * attempt)
    return False


async def _get_all_bookings() -> list[dict]:
    mongo_items = await _get_mongo_bookings()
    if IS_PRODUCTION:
        return mongo_items
    local_items = _load_local_bookings()
    return _merge_bookings(local_items, mongo_items)


async def _sync_local_bookings_to_mongo() -> None:
    if db is None:
        return
    local_items = _load_local_bookings()
    if not local_items:
        return
    synced = 0
    for item in local_items:
        booking_id = item.get("booking_id")
        if not booking_id:
            continue
        try:
            await db.bookings.replace_one(
                {'booking_id': booking_id},
                item.copy(),
                upsert=True,
            )
            synced += 1
        except PyMongoError as e:
            logger.error("Could not sync booking %s to MongoDB: %s", booking_id, e)
    if synced:
        logger.info("Synced %s local booking(s) to MongoDB", synced)


def _create_admin_token() -> str:
    issued = int(datetime.now(timezone.utc).timestamp())
    payload = f"adm:{issued}"
    sig = hmac.new(ADMIN_SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def _verify_admin_token(token: str) -> bool:
    if not token or "." not in token:
        return False
    payload, sig = token.rsplit(".", 1)
    expected = hmac.new(ADMIN_SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected) or not payload.startswith("adm:"):
        return False
    try:
        issued = int(payload.split(":", 1)[1])
    except ValueError:
        return False
    age = datetime.now(timezone.utc).timestamp() - issued
    return age <= ADMIN_SESSION_MAX_AGE


async def _mongo_ping_ok() -> bool:
    if client is None:
        return False
    try:
        await client.admin.command("ping")
        return True
    except Exception:
        return False


async def _update_booking_status(booking_id: str, status: str) -> dict:
    updated: Optional[dict] = None

    if db is not None:
        try:
            existing = await db.bookings.find_one({'booking_id': booking_id}, {'_id': 0})
            if existing:
                existing['status'] = status
                await db.bookings.replace_one({'booking_id': booking_id}, existing, upsert=True)
                updated = existing
        except PyMongoError as e:
            logger.error("Could not update booking in MongoDB: %s", e)
            if IS_PRODUCTION:
                raise HTTPException(503, "Database unavailable")

    if not IS_PRODUCTION:
        local_items = _load_local_bookings()
        for idx, item in enumerate(local_items):
            if item.get("booking_id") == booking_id:
                local_items[idx]["status"] = status
                updated = local_items[idx].copy()
                _save_local_bookings(local_items)
                break
        if updated and db is not None and not await _mongo_ping_ok():
            try:
                await db.bookings.replace_one({'booking_id': booking_id}, updated, upsert=True)
            except PyMongoError:
                pass

    if not updated:
        raise HTTPException(404, "Booking not found")
    return updated

# ---------- Models ----------
class BookingCreate(BaseModel):
    customer_name: str
    phone: str
    email: Optional[EmailStr] = None
    service_id: str
    service_title: str
    address: str
    city: Optional[str] = None
    pincode: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    preferred_time: Optional[str] = None
    notes: Optional[str] = None


class Booking(BookingCreate):
    booking_id: str
    status: Literal['pending', 'in_progress', 'completed', 'cancelled'] = 'pending'
    created_at: str


class StatusUpdate(BaseModel):
    status: Literal['pending', 'in_progress', 'completed', 'cancelled']


class User(BaseModel):
    user_id: str
    email: str
    name: str
    picture: Optional[str] = None
    role: str = 'customer'

# ---------- Services Catalog ----------
SERVICES = [
    {'id': 'fan-repair', 'title': 'Fan Repair & Install', 'icon': 'Fan', 'price_from': 199, 'duration': '30-60 min', 'description': 'Ceiling/wall/exhaust fan repair, balancing & installation.'},
    {'id': 'switch-socket', 'title': 'Switch & Socket', 'icon': 'Plugs', 'price_from': 99, 'duration': '20-40 min', 'description': 'Switch, socket and modular plate replacement or repair.'},
    {'id': 'wiring', 'title': 'Wiring & Rewiring', 'icon': 'Lightning', 'price_from': 499, 'duration': '1-3 hrs', 'description': 'New wiring, fault finding and complete rewiring jobs.'},
    {'id': 'ac-repair', 'title': 'AC Repair & Service', 'icon': 'Snowflake', 'price_from': 399, 'duration': '45-90 min', 'description': 'AC servicing, gas refill, installation and repairs.'},
    {'id': 'geyser', 'title': 'Geyser / Water Heater', 'icon': 'Drop', 'price_from': 249, 'duration': '30-60 min', 'description': 'Geyser installation, leak fix and element replacement.'},
    {'id': 'inverter', 'title': 'Inverter & Battery', 'icon': 'BatteryCharging', 'price_from': 299, 'duration': '30-60 min', 'description': 'Inverter setup, battery replacement and load checks.'},
    {'id': 'lighting', 'title': 'Lights & Chandeliers', 'icon': 'Lightbulb', 'price_from': 149, 'duration': '20-60 min', 'description': 'LED, fancy lights & chandelier installation.'},
    {'id': 'mcb-fuse', 'title': 'MCB / Fuse / DB', 'icon': 'ShieldCheck', 'price_from': 199, 'duration': '30-45 min', 'description': 'MCB tripping, fuse replacement, DB box repair.'},
    {'id': 'doorbell', 'title': 'Doorbell & Smart', 'icon': 'Bell', 'price_from': 149, 'duration': '20-40 min', 'description': 'Doorbell, video bell & smart switch installs.'},
    {'id': 'appliance', 'title': 'Appliance Install', 'icon': 'Television', 'price_from': 199, 'duration': '30-60 min', 'description': 'TV mounting, washing machine, microwave install.'},
    {'id': 'stabilizer', 'title': 'Stabilizer', 'icon': 'GaugeCircle', 'price_from': 199, 'duration': '20-40 min', 'description': 'Stabilizer install, repair and replacement.'},
    {'id': 'emergency', 'title': 'Emergency 24x7', 'icon': 'Siren', 'price_from': 599, 'duration': 'ASAP', 'description': 'Power outage, short circuit or any urgent fault.'},
]


# ---------- Auth ----------
async def get_current_user(request: Request) -> User:
    token = request.cookies.get('session_token')
    if not token:
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header.split(' ', 1)[1]
    if not token:
        raise HTTPException(status_code=401, detail='Not authenticated')
    if db is None:
        raise HTTPException(status_code=500, detail='Database not configured')
    session = await db.user_sessions.find_one({'session_token': token}, {'_id': 0})
    if not session:
        raise HTTPException(status_code=401, detail='Invalid session')
    expires_at = session.get('expires_at')
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail='Session expired')
    user = await db.users.find_one({'user_id': session['user_id']}, {'_id': 0})
    if not user:
        raise HTTPException(status_code=401, detail='User not found')
    return User(**user)


async def get_admin_user(user: User = Depends(get_current_user)) -> User:
    if user.email.lower() != ADMIN_EMAIL or user.role != 'admin':
        raise HTTPException(status_code=403, detail='Admin access required')
    return user


async def get_optional_user(request: Request) -> Optional[User]:
    try:
        return await get_current_user(request)
    except HTTPException:
        return None


def _is_portal_admin(request: Request) -> bool:
    token = request.cookies.get("admin_portal_token")
    return _verify_admin_token(token or "")


async def require_admin_access(
    request: Request,
    user: Optional[User] = Depends(get_optional_user),
) -> dict:
    if _is_portal_admin(request):
        return {"mode": "pin", "email": "portal-admin@local"}
    if user and user.email.lower() == ADMIN_EMAIL and user.role == "admin":
        return {"mode": "oauth", "email": user.email}
    raise HTTPException(status_code=403, detail="Admin access required")


# ---------- Email ----------
def _build_email_html(b: dict) -> str:
    map_link = ''
    if b.get('latitude') and b.get('longitude'):
        map_link = f"<a href='https://www.google.com/maps?q={b['latitude']},{b['longitude']}' style='color:#0055FF'>View on Google Maps</a>"
    return f"""
    <table style='font-family:Arial,sans-serif;max-width:600px;border:2px solid #0A0A0A;padding:24px;background:#fff'>
      <tr><td>
        <h2 style='margin:0 0 8px;color:#0055FF;letter-spacing:-0.02em'>⚡ NEW BOOKING</h2>
        <p style='margin:0 0 16px;color:#4B5563'>A customer just requested an electrical service.</p>
        <hr style='border:none;border-top:2px solid #0A0A0A;margin:16px 0'/>
        <p><strong>Service:</strong> {b.get('service_title')}</p>
        <p><strong>Customer:</strong> {b.get('customer_name')}</p>
        <p><strong>Phone:</strong> <a href='tel:{b.get('phone')}'>{b.get('phone')}</a></p>
        <p><strong>Email:</strong> {b.get('email') or '-'}</p>
        <p><strong>Address:</strong> {b.get('address')}</p>
        <p><strong>City / Pin:</strong> {b.get('city') or '-'} / {b.get('pincode') or '-'}</p>
        <p><strong>Preferred Time:</strong> {b.get('preferred_time') or 'Anytime'}</p>
        <p><strong>Notes:</strong> {b.get('notes') or '-'}</p>
        <p><strong>Location:</strong> {map_link or 'Not shared'}</p>
        <p><strong>Booking ID:</strong> {b.get('booking_id')}</p>
      </td></tr>
    </table>
    """


async def send_admin_email(booking: dict):
    if not RESEND_API_KEY:
        logging.warning('RESEND_API_KEY not set - skipping email notification')
        return
    try:
        params = {
            'from': SENDER_EMAIL,
            'to': [ADMIN_EMAIL],
            'subject': f"⚡ New Booking: {booking.get('service_title')} — {booking.get('customer_name')}",
            'html': _build_email_html(booking),
        }
        await asyncio.to_thread(resend.Emails.send, params)
    except Exception as e:
        logging.error(f'Email send failed: {e}')


# ---------- Routes ----------
@api_router.get('/')
async def root():
    return {'message': 'EVOLT Electric API', 'status': 'ok'}


@api_router.get('/health')
async def health_check():
    mongo_ok = await _mongo_ping_ok()
    items = await _get_all_bookings()
    mongo_count = len(await _get_mongo_bookings()) if mongo_ok else 0
    return {
        'status': 'ok',
        'mongodb': 'connected' if mongo_ok else 'unavailable',
        'storage': 'mongodb' if (IS_PRODUCTION and mongo_ok) else ('local_fallback' if not IS_PRODUCTION else 'mongodb_required'),
        'bookings_total': len(items),
        'bookings_in_mongodb': mongo_count,
        'production': IS_PRODUCTION,
    }


@api_router.get('/services')
async def list_services():
    return {'services': SERVICES, 'whatsapp': WHATSAPP_NUMBER}


@api_router.post('/bookings', response_model=Booking)
async def create_booking(payload: BookingCreate):
    booking_id = f"bk_{uuid.uuid4().hex[:10]}"
    doc = payload.model_dump()
    doc['booking_id'] = booking_id
    doc['status'] = 'pending'
    doc['created_at'] = datetime.now(timezone.utc).isoformat()
    save_doc = doc.copy()
    save_doc.pop('_id', None)

    mongo_saved = await _save_booking_to_mongo(save_doc)

    if IS_PRODUCTION:
        if not mongo_saved:
            raise HTTPException(
                status_code=503,
                detail="Database unavailable. Booking was NOT saved. Please try again in a moment.",
            )
    else:
        local_items = _load_local_bookings()
        local_items.insert(0, save_doc.copy())
        _save_local_bookings(local_items)
        if db is not None and not mongo_saved:
            asyncio.create_task(_sync_local_bookings_to_mongo())

    asyncio.create_task(send_admin_email(save_doc))
    return Booking(**save_doc)


@api_router.get('/bookings')
async def list_bookings(_: dict = Depends(require_admin_access)):
    items = await _get_all_bookings()
    return {'bookings': items}


@api_router.patch('/bookings/{booking_id}/status', response_model=Booking)
async def update_status(booking_id: str, body: StatusUpdate, _: dict = Depends(require_admin_access)):
    updated = await _update_booking_status(booking_id, body.status)
    return Booking(**updated)


@api_router.get('/bookings/stats')
async def stats(_: dict = Depends(require_admin_access)):
    items = await _get_all_bookings()
    return _stats_from_items(items)


# ---------- Auth Endpoints ----------
@api_router.post('/auth/session')
async def auth_session(request: Request, response: Response):
    body = await request.json()
    session_id = body.get('session_id')
    if not session_id:
        raise HTTPException(400, 'session_id required')
    async with httpx.AsyncClient(timeout=15) as http:
        r = await http.get(
            'https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data',
            headers={'X-Session-ID': session_id},
        )
        if r.status_code != 200:
            raise HTTPException(401, 'Invalid session_id')
        data = r.json()

    email = data['email'].lower()
    role = 'admin' if email == ADMIN_EMAIL else 'customer'
    if db is not None:
        existing = await db.users.find_one({'email': email}, {'_id': 0})
    else:
        existing = None
    if existing:
        user_id = existing['user_id']
        await db.users.update_one(
            {'user_id': user_id},
            {'$set': {'name': data.get('name'), 'picture': data.get('picture'), 'role': role}},
        )
    else:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        if db is not None:
            await db.users.insert_one({
                'user_id': user_id,
                'email': email,
                'name': data.get('name', ''),
                'picture': data.get('picture', ''),
                'role': role,
                'created_at': datetime.now(timezone.utc).isoformat(),
            })
    session_token = data["session_token"]
    expires_at = datetime.now(timezone.utc) + timedelta(days=7)
    if db is not None:
        await db.user_sessions.insert_one({
            "user_id": user_id,
            "session_token": session_token,
            "expires_at": expires_at.isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    response.set_cookie(
        key="session_token",
        value=session_token,
        max_age=7 * 24 * 60 * 60,
        httponly=True,
        secure=True,
        samesite="none",
        path="/",
    )
    user_doc = None
    if db is not None:
        user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    return {"user": user_doc, "session_token": session_token}


@api_router.get("/auth/me")
async def auth_me(user: User = Depends(get_current_user)):
    is_admin = user.email.lower() == ADMIN_EMAIL and user.role == "admin"
    return {"user": user.model_dump(), "is_admin": is_admin}


@api_router.post("/auth/logout")
async def auth_logout(request: Request, response: Response):
    token = request.cookies.get("session_token")
    if token and db is not None:
        await db.user_sessions.delete_one({"session_token": token})
    response.delete_cookie("session_token", path="/")
    return {"ok": True}


@api_router.post("/admin/login")
async def admin_login(request: Request, response: Response):
    body = await request.json()
    pin = body.get("pin", "")
    if not pin:
        raise HTTPException(status_code=400, detail="pin required")
    if pin != ADMIN_PORTAL_PIN:
        raise HTTPException(status_code=401, detail="Invalid PIN")

    token = _create_admin_token()
    response.set_cookie(
        key="admin_portal_token",
        value=token,
        max_age=ADMIN_SESSION_MAX_AGE,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="lax",
        path="/",
    )
    return {"ok": True}


@api_router.post("/admin/logout")
async def admin_logout(request: Request, response: Response):
    response.delete_cookie("admin_portal_token", path="/")
    return {"ok": True}


@api_router.get("/admin/me")
async def admin_me(_: dict = Depends(require_admin_access)):
    return {"ok": True}


@api_router.post("/admin/cleanup")
async def admin_cleanup(_: dict = Depends(require_admin_access)):
    """Delete expired sessions and orphaned data"""
    cleanup_stats = {
        'expired_sessions_deleted': 0,
        'status': 'ok'
    }
    
    if db is not None:
        try:
            # Delete expired user sessions
            result = await db.user_sessions.delete_many({
                'expires_at': {
                    '$lt': datetime.now(timezone.utc).isoformat()
                }
            })
            cleanup_stats['expired_sessions_deleted'] = result.deleted_count
        except PyMongoError as e:
            logger.error("Cleanup failed: %s", e)
            cleanup_stats['status'] = 'partial_error'
    
    return cleanup_stats


app.include_router(api_router)

@app.get("/")
async def serve_frontend():
    return FileResponse(ROOT_DIR / "customer.html")


@app.head("/")
async def head_frontend():
    return Response(status_code=200)


@app.get("/admin")
async def serve_admin_frontend():
    return FileResponse(ROOT_DIR / "admin.html")


@app.head("/admin")
async def head_admin_frontend():
    return Response(status_code=200)

cors_origins = os.environ.get('CORS_ORIGINS', '*')
cors_origins_list = [origin.strip() for origin in cors_origins.split(',')] if cors_origins != '*' else ['*']

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=cors_origins_list,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_sync_bookings():
    if db is not None:
        try:
            await db.bookings.create_index('booking_id', unique=True)
            await db.bookings.create_index('created_at')
            logger.info("MongoDB booking indexes ready")
        except PyMongoError as e:
            logger.error("Could not create MongoDB indexes: %s", e)
    await _sync_local_bookings_to_mongo()
    mongo_ok = await _mongo_ping_ok()
    if IS_PRODUCTION and not mongo_ok:
        logger.error("PRODUCTION WARNING: MongoDB is not connected. Bookings will NOT be saved permanently!")
    elif mongo_ok:
        count = len(await _get_mongo_bookings())
        logger.info("MongoDB connected. %s booking(s) in permanent storage.", count)


@app.on_event("shutdown")
async def shutdown_db_client():
    if client is not None:
        client.close() 