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
import secrets
import json
import httpx
import resend
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional, Literal
from datetime import datetime, timezone, timedelta

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ.get('MONGO_URL', '')
client = (
    AsyncIOMotorClient(
        mongo_url,
        serverSelectionTimeoutMS=3000,
        connectTimeoutMS=3000,
        socketTimeoutMS=3000,
    )
    if mongo_url
    else None
)
db = client[os.environ['DB_NAME']] if client and os.environ.get('DB_NAME') else None

ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'ayushsahu05246@gmail.com').lower()
RESEND_API_KEY = os.environ.get('RESEND_API_KEY', '')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL', 'onboarding@resend.dev')
WHATSAPP_NUMBER = os.environ.get('WHATSAPP_NUMBER', '9454386338')
ADMIN_PORTAL_PIN = os.environ.get('ADMIN_PORTAL_PIN', '1234')
admin_portal_sessions = set()
BOOKINGS_FILE = ROOT_DIR / "bookings.local.json"

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
    return bool(token and token in admin_portal_sessions)


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

    # Always keep a local copy so app works without MongoDB.
    local_items = _load_local_bookings()
    local_items.insert(0, doc.copy())
    _save_local_bookings(local_items)

    if db is not None:
        try:
            await db.bookings.insert_one(doc)
        except PyMongoError as e:
            logger.error("Booking save skipped (DB unavailable): %s", e)
    doc.pop('_id', None)
    asyncio.create_task(send_admin_email(doc))
    return Booking(**doc)


@api_router.get('/bookings')
async def list_bookings(_: dict = Depends(require_admin_access)):
    if db is None:
        return {'bookings': _load_local_bookings()}
    try:
        items = await db.bookings.find({}, {'_id': 0}).sort('created_at', -1).to_list(2000)
        return {'bookings': items}
    except PyMongoError as e:
        logger.error("Could not load bookings (DB unavailable): %s", e)
        return {'bookings': _load_local_bookings()}


@api_router.patch('/bookings/{booking_id}/status', response_model=Booking)
async def update_status(booking_id: str, body: StatusUpdate, _: dict = Depends(require_admin_access)):
    if db is None:
        local_items = _load_local_bookings()
        for idx, item in enumerate(local_items):
            if item.get("booking_id") == booking_id:
                local_items[idx]["status"] = body.status
                _save_local_bookings(local_items)
                return Booking(**local_items[idx])
        raise HTTPException(404, 'Booking not found')
    try:
        res = await db.bookings.find_one_and_update(
            {'booking_id': booking_id},
            {'$set': {'status': body.status}},
            return_document=True,
            projection={'_id': 0},
        )
        if res:
            local_items = _load_local_bookings()
            for idx, item in enumerate(local_items):
                if item.get("booking_id") == booking_id:
                    local_items[idx]["status"] = body.status
                    _save_local_bookings(local_items)
                    break
    except PyMongoError as e:
        logger.error("Could not update booking status (DB unavailable): %s", e)
        local_items = _load_local_bookings()
        for idx, item in enumerate(local_items):
            if item.get("booking_id") == booking_id:
                local_items[idx]["status"] = body.status
                _save_local_bookings(local_items)
                return Booking(**local_items[idx])
        raise HTTPException(503, 'Database unavailable')
    if not res:
        raise HTTPException(404, 'Booking not found')
    return Booking(**res)


@api_router.get('/bookings/stats')
async def stats(_: dict = Depends(require_admin_access)):
    if db is None:
        items = _load_local_bookings()
        return {
            'total': len(items),
            'pending': sum(1 for x in items if x.get("status") == "pending"),
            'in_progress': sum(1 for x in items if x.get("status") == "in_progress"),
            'completed': sum(1 for x in items if x.get("status") == "completed"),
        }
    try:
        total = await db.bookings.count_documents({})
        pending = await db.bookings.count_documents({'status': 'pending'})
        in_progress = await db.bookings.count_documents({'status': 'in_progress'})
        completed = await db.bookings.count_documents({'status': 'completed'})
        return {'total': total, 'pending': pending, 'in_progress': in_progress, 'completed': completed}
    except PyMongoError as e:
        logger.error("Could not load stats (DB unavailable): %s", e)
        items = _load_local_bookings()
        return {
            'total': len(items),
            'pending': sum(1 for x in items if x.get("status") == "pending"),
            'in_progress': sum(1 for x in items if x.get("status") == "in_progress"),
            'completed': sum(1 for x in items if x.get("status") == "completed"),
        }


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

    token = f"adm_{secrets.token_hex(16)}"
    admin_portal_sessions.add(token)
    response.set_cookie(
        key="admin_portal_token",
        value=token,
        max_age=7 * 24 * 60 * 60,
        httponly=True,
        secure=False,
        samesite="lax",
        path="/",
    )
    return {"ok": True}


@api_router.post("/admin/logout")
async def admin_logout(request: Request, response: Response):
    token = request.cookies.get("admin_portal_token")
    if token and token in admin_portal_sessions:
        admin_portal_sessions.remove(token)
    response.delete_cookie("admin_portal_token", path="/")
    return {"ok": True}


@api_router.get("/admin/me")
async def admin_me(_: dict = Depends(require_admin_access)):
    return {"ok": True}


app.include_router(api_router)

@app.get("/")
async def serve_frontend():
    return FileResponse(ROOT_DIR / "customer.html")


@app.get("/admin")
async def serve_admin_frontend():
    return FileResponse(ROOT_DIR / "admin.html")

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@app.on_event("shutdown")
async def shutdown_db_client():
    if client is not None:
        client.close()
