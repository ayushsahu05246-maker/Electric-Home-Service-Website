# Deploy on Render (FastAPI + HTML)

## 1) Push project to GitHub
Keep these files in repo:
- `server.py`
- `customer.html`
- `admin.html`
- `requirements.txt`
- `.gitignore`

Do not push secrets (`.env` is ignored).

## 2) Create Render service
1. Open [https://render.com](https://render.com)
2. New -> Web Service
3. Connect your GitHub repo
4. Select branch (usually `main`)
5. Runtime: `Python`

## 3) Build and start commands
- Build Command:
  `pip install -r requirements.txt`
- Start Command:
  `uvicorn server:app --host 0.0.0.0 --port $PORT`

## 4) Add environment variables in Render dashboard
Set these in Render -> Environment:
- `MONGO_URL`
- `DB_NAME`
- `ADMIN_EMAIL`
- `ADMIN_PORTAL_PIN`
- `CORS_ORIGINS` (set your Render app URL)
- `RESEND_API_KEY` (optional)
- `SENDER_EMAIL`
- `WHATSAPP_NUMBER`

Example `CORS_ORIGINS`:
`https://your-app-name.onrender.com`

## 5) Deploy and test
After deploy:
- Customer site: `https://your-app-name.onrender.com/`
- Admin: `https://your-app-name.onrender.com/admin`

Test flow:
1. Submit a booking from customer page
2. Login admin with `ADMIN_PORTAL_PIN`
3. Verify booking appears in dashboard

## 6) If Atlas fails in production
Check in MongoDB Atlas:
1. Network Access includes your Render outbound IP strategy (or temporary allow all for testing)
2. Database user/password in `MONGO_URL` are correct
3. Connection string copied from Atlas "Connect -> Drivers"
