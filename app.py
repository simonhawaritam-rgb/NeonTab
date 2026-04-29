import os, json, re, time, logging, secrets, hashlib, threading, random, string
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, session, send_from_directory, Response
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import yt_dlp

logging.basicConfig(level=logging.WARNING)
app = Flask(__name__)
app.secret_key = 'neon-secret-fast'
CORS(app)

# Download folder for High-quality mode
DOWNLOAD_FOLDER = 'downloads'
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Job tracking
download_jobs = {}
job_lock = threading.Lock()

# -------------------------------
# Database setup
# -------------------------------
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# -------------------------------
# Models
# -------------------------------
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), unique=True, nullable=False)
    balance = db.Column(db.Float, default=0.0)
    expires_at = db.Column(db.DateTime, nullable=True)
    device_id = db.Column(db.String(64), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_admin = db.Column(db.Boolean, default=False)

    def get_id(self):
        return str(self.id)

class StreamLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    video_title = db.Column(db.String(500))
    video_url = db.Column(db.String(500))
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

with app.app_context():
    db.create_all()
    admin = User.query.filter_by(is_admin=True).first()
    if not admin:
        admin = User(username='ADMIN-00', balance=9999, is_admin=True)
        db.session.add(admin)
        db.session.commit()

# -------------------------------
# Config & Cache Helpers
# -------------------------------
CONFIG_FILE = 'config.json'
CACHE_FILE = 'links.json'
HIGH_CACHE_FILE = 'highlinks.json'
BROADCAST_FILE = 'notification_broadcast.json'
CACHE_TTL = 43200
HIGH_CACHE_TTL = 86400

DEFAULT_CONFIG = {
    "site_title": "NeonTube",
    "broadcast_message": "",
    "maintenance_mode": "off",
    "maintenance_message": "",
    "auth_required": True
}

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(cfg, f)

if not os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(DEFAULT_CONFIG, f)

def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def clean_expired_cache():
    cache = load_cache()
    now = time.time()
    changed = False
    for vid in list(cache.keys()):
        if cache[vid].get('expires_at', 0) < now:
            del cache[vid]
            changed = True
    if changed:
        save_cache(cache)

def load_high_cache():
    if not os.path.exists(HIGH_CACHE_FILE):
        return {}
    try:
        with open(HIGH_CACHE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_high_cache(cache):
    with open(HIGH_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def clean_expired_high_cache():
    cache = load_high_cache()
    now = time.time()
    changed = False
    for vid in list(cache.keys()):
        if cache[vid].get('expires_at', 0) < now:
            local_file = cache[vid].get('stream_url', '').replace('/downloads/', '')
            if local_file and os.path.exists(os.path.join(DOWNLOAD_FOLDER, local_file)):
                try: os.remove(os.path.join(DOWNLOAD_FOLDER, local_file))
                except: pass
            del cache[vid]
            changed = True
    if changed:
        save_high_cache(cache)

def extract_video_id(url):
    patterns = [r'(?:v=|\/)([0-9A-Za-z_-]{11})(?:[?&]|$)', r'youtu\.be\/([0-9A-Za-z_-]{11})']
    for p in patterns:
        m = re.search(p, url)
        if m: return m.group(1)
    return None

# -------------------------------
# Notification Broadcast
# -------------------------------
def load_broadcast():
    if not os.path.exists(BROADCAST_FILE):
        return {"message": "", "timestamp": 0}
    try:
        with open(BROADCAST_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"message": "", "timestamp": 0}

def save_broadcast(data):
    with open(BROADCAST_FILE, 'w') as f:
        json.dump(data, f)

# -------------------------------
# Device Fingerprint (Cloudflare-aware)
# -------------------------------
def generate_device_fingerprint():
    user_agent = request.headers.get('User-Agent', '')
    ip = request.headers.get('CF-Connecting-IP') or request.headers.get('X-Forwarded-For', request.remote_addr)
    accept_lang = request.headers.get('Accept-Language', '')
    raw = f"{ip}|{user_agent}|{accept_lang}"
    return hashlib.sha256(raw.encode()).hexdigest()

# -------------------------------
# Background Tasks
# -------------------------------
def deduct_daily_balances():
    with app.app_context():
        users = User.query.filter(User.is_admin == False).all()
        for user in users:
            if user.expires_at and user.expires_at < datetime.utcnow():
                continue
            if user.balance >= 0.36:
                user.balance -= 0.36
            else:
                user.balance = 0
        db.session.commit()

def clear_all_device_bindings():
    with app.app_context():
        users = User.query.filter(User.is_admin == False).all()
        for user in users:
            user.device_id = None
        db.session.commit()
        app.logger.info("Cleared device bindings for all users")

def start_background_tasks():
    def daily_loop():
        while True:
            time.sleep(86400)
            deduct_daily_balances()
    def device_clear_loop():
        while True:
            time.sleep(1800)
            clear_all_device_bindings()
    threading.Thread(target=daily_loop, daemon=True).start()
    threading.Thread(target=device_clear_loop, daemon=True).start()

start_background_tasks()

# -------------------------------
# Routes: Main & Auth
# -------------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/sw.js')
def service_worker():
    return send_from_directory('static', 'sw.js', mimetype='application/javascript')

@app.route('/api/auth/verify', methods=['POST'])
def verify_auth():
    cfg = load_config()
    if not cfg.get('auth_required', True):
        return jsonify({'valid': True, 'user': {'username': 'Guest', 'balance': 0, 'expires_at': None, 'device_id': 'guest', 'is_admin': False}})
    data = request.json
    username = data.get('username')
    client_device_id = data.get('device_id')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'valid': False})
    if user.expires_at and user.expires_at < datetime.utcnow():
        return jsonify({'valid': False, 'error': 'Account expired'})
    if user.device_id is not None and user.device_id != client_device_id:
        return jsonify({'valid': False, 'error': 'Device mismatch'})
    if user.device_id is None:
        user.device_id = client_device_id
        db.session.commit()
    login_user(user)
    return jsonify({'valid': True, 'user': {
        'username': user.username,
        'balance': user.balance,
        'expires_at': user.expires_at.isoformat() if user.expires_at else None,
        'device_id': user.device_id,
        'is_admin': user.is_admin
    }})

@app.route('/api/auth/login', methods=['POST'])
def api_login():
    cfg = load_config()
    if not cfg.get('auth_required', True):
        return jsonify({'success': True, 'user': {'username': 'Guest', 'balance': 0, 'expires_at': None, 'device_id': 'guest', 'is_admin': False}})
    data = request.json
    username = data.get('username', '').strip().upper()
    client_device_id = data.get('device_id')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'success': False, 'error': 'User not found'})
    if user.expires_at and user.expires_at < datetime.utcnow():
        return jsonify({'success': False, 'error': 'Account expired'})
    if not user.is_admin:
        if user.device_id is None:
            user.device_id = client_device_id
            db.session.commit()
        elif user.device_id != client_device_id:
            return jsonify({'success': False, 'error': 'This account is bound to another device.'})
    login_user(user)
    return jsonify({'success': True, 'user': {
        'username': user.username,
        'balance': user.balance,
        'expires_at': user.expires_at.isoformat() if user.expires_at else None,
        'device_id': user.device_id,
        'is_admin': user.is_admin
    }})

@app.route('/api/admin/login', methods=['POST'])
def api_admin_login():
    data = request.json
    if data.get('password') == 'ADMIN707':
        admin = User.query.filter_by(is_admin=True).first()
        if not admin:
            admin = User(username='ADMIN-00', balance=9999, is_admin=True)
            db.session.add(admin)
            db.session.commit()
        login_user(admin)
        return jsonify({'success': True, 'user': {'username': admin.username, 'is_admin': True}})
    return jsonify({'success': False, 'error': 'Invalid password'})

@app.route('/api/logout', methods=['POST'])
@login_required
def api_logout():
    logout_user()
    return jsonify({'success': True})

# -------------------------------
# Stream API (Protected)
# -------------------------------
@app.route('/api/stream', methods=['POST'])
def stream():
    cfg = load_config()
    auth_required = cfg.get('auth_required', True)
    data = request.json
    url = data.get('url')
    username = data.get('username')
    device_id = data.get('device_id')
    mode = data.get('mode', 'normal')

    if auth_required:
        if not username or not device_id:
            return jsonify({'error': 'Authentication required'}), 401
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'error': 'Invalid user'}), 401
        if not user.is_admin and user.device_id != device_id:
            return jsonify({'error': 'Device mismatch'}), 401
        if not user.is_admin:
            if user.expires_at and user.expires_at < datetime.utcnow():
                return jsonify({'error': 'Account expired'}), 403
            if user.balance < 0.36:
                return jsonify({'error': 'Insufficient balance. Please top up.'}), 402
    else:
        user = None

    vid = extract_video_id(url)
    now = time.time()

    # Normal mode (unchanged)
    if mode == 'normal':
        clean_expired_cache()
        cache = load_cache()
        if vid and vid in cache and cache[vid].get('expires_at', 0) > now:
            if auth_required and user:
                log = StreamLog(user_id=user.id, video_title=cache[vid]['title'], video_url=url)
                db.session.add(log); db.session.commit()
            return jsonify({**cache[vid], 'cached': True, 'mode': 'normal'})
        ydl_opts = {
            'quiet': True, 'no_warnings': True,
            'format': 'bestvideo[height<=720]+bestaudio/best[height<=1080]/best',
            'socket_timeout': 10, 'retries': 1, 'fragment_retries': 1,
            'skip_download': True, 'ignoreerrors': True,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                title = info.get('title', 'Unknown')
                thumbnail = info.get('thumbnail', '')
                duration = info.get('duration', 0)
                stream_url = info.get('url')
                if not stream_url:
                    formats = info.get('formats', [])
                    for f in formats:
                        if f.get('vcodec') != 'none' and f.get('acodec') != 'none' and f.get('height', 9999) <= 1080:
                            stream_url = f.get('url'); break
                    if not stream_url and formats:
                        stream_url = formats[-1].get('url')
                if not stream_url: raise Exception("No streamable URL found")
            result = {'title': title, 'thumbnail': thumbnail, 'duration': duration, 'stream_url': stream_url, 'cached': False, 'mode': 'normal'}
            if vid:
                cache[vid] = {**result, 'expires_at': now + CACHE_TTL}
                save_cache(cache)
            if auth_required and user:
                log = StreamLog(user_id=user.id, video_title=title, video_url=url)
                db.session.add(log); db.session.commit()
            return jsonify(result)
        except Exception as e:
            return jsonify({'error': f'Extraction failed: {str(e)}'}), 500

    # High mode (async download)
    elif mode == 'high':
        clean_expired_high_cache()
        high_cache = load_high_cache()
        if vid and vid in high_cache and high_cache[vid].get('expires_at', 0) > now:
            local_file = high_cache[vid]['stream_url'].replace('/downloads/', '')
            if os.path.exists(os.path.join(DOWNLOAD_FOLDER, local_file)):
                if auth_required and user:
                    log = StreamLog(user_id=user.id, video_title=high_cache[vid]['title'], video_url=url)
                    db.session.add(log); db.session.commit()
                return jsonify({**high_cache[vid], 'cached': True, 'mode': 'high'})

        # Start download in background
        job_id = vid or f'job_{int(time.time())}'
        with job_lock:
            download_jobs[job_id] = {'status': 'starting', 'percent': 0, 'eta': 0, 'downloaded_bytes': 0, 'total_bytes': 0, 'title': '', 'thumbnail': '', 'duration': 0, 'stream_url': '', 'error': None}

        def progress_hook(d):
            if d['status'] == 'downloading':
                percent = 0
                try: percent = float(d.get('_percent_str', '0%').replace('%', ''))
                except: pass
                eta = d.get('eta', 0) or 0
                downloaded = d.get('downloaded_bytes', 0)
                total = d.get('total_bytes', 0) or d.get('total_bytes_estimate', 0)
                with job_lock:
                    download_jobs[job_id].update({
                        'status': 'downloading', 'percent': min(percent, 100), 'eta': eta,
                        'downloaded_bytes': downloaded, 'total_bytes': total
                    })
            elif d['status'] == 'finished':
                with job_lock:
                    download_jobs[job_id]['status'] = 'finished'

        def download_task():
            try:
                for f in os.listdir(DOWNLOAD_FOLDER):
                    fpath = os.path.join(DOWNLOAD_FOLDER, f)
                    if os.path.isfile(fpath) and time.time() - os.path.getmtime(fpath) > 3600:
                        os.remove(fpath)
                safe_title = re.sub(r'[^\w\s-]', '', vid)[:50] if vid else 'video'
                outtmpl = os.path.join(DOWNLOAD_FOLDER, f'{vid}_{int(time.time())}.%(ext)s')
                merged_file = outtmpl.replace('.%(ext)s', '.mp4')
                ffmpeg_available = bool(yt_dlp.utils.check_executable('ffmpeg'))
                ydl_opts = {
                    'quiet': True, 'no_warnings': True,
                    'format': 'bestvideo[height<=720]+bestaudio/best[height<=720]' if ffmpeg_available else 'best[height<=720]/best[height<=1080]',
                    'merge_output_format': 'mp4' if ffmpeg_available else None,
                    'outtmpl': outtmpl,
                    'progress_hooks': [progress_hook],
                    'socket_timeout': 30, 'retries': 3,
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    title = info.get('title', 'Unknown')
                    thumbnail = info.get('thumbnail', '')
                    duration = info.get('duration', 0)
                if not os.path.exists(merged_file):
                    possible = [f for f in os.listdir(DOWNLOAD_FOLDER) if f.startswith(vid)]
                    if possible: merged_file = os.path.join(DOWNLOAD_FOLDER, possible[0])
                local_url = f'/downloads/{os.path.basename(merged_file)}'
                with job_lock:
                    download_jobs[job_id].update({
                        'status': 'done', 'percent': 100, 'eta': 0,
                        'title': title, 'thumbnail': thumbnail, 'duration': duration, 'stream_url': local_url
                    })
                # Save to high cache
                if vid:
                    high_cache = load_high_cache()
                    high_cache[vid] = {'title': title, 'thumbnail': thumbnail, 'duration': duration, 'stream_url': local_url, 'expires_at': now + HIGH_CACHE_TTL}
                    save_high_cache(high_cache)
                if auth_required and user:
                    with app.app_context():
                        log = StreamLog(user_id=user.id, video_title=title, video_url=url)
                        db.session.add(log); db.session.commit()
            except Exception as e:
                with job_lock:
                    download_jobs[job_id].update({'status': 'error', 'error': str(e)})

        thread = threading.Thread(target=download_task, daemon=True)
        thread.start()
        return jsonify({'job_id': job_id, 'status': 'started', 'mode': 'high'})

# Progress endpoint
@app.route('/api/stream/progress/<job_id>')
def stream_progress(job_id):
    with job_lock:
        job = download_jobs.get(job_id, {})
    return jsonify(job)

# Completed job info endpoint
@app.route('/api/stream/result/<job_id>')
def stream_result(job_id):
    with job_lock:
        job = download_jobs.get(job_id, {})
    if job.get('status') == 'done':
        return jsonify(job)
    return jsonify({'error': 'Not ready'}), 404

# Serve downloaded files
@app.route('/downloads/<filename>')
def download_file(filename):
    return send_from_directory(DOWNLOAD_FOLDER, filename)

# -------------------------------
# Admin API (User Management)
# -------------------------------
def admin_required(f):
    def wrap(*a, **k):
        if not (session.get('admin') or (current_user.is_authenticated and current_user.is_admin)):
            return jsonify({'error': 'Admin required'}), 403
        return f(*a, **k)
    wrap.__name__ = f.__name__
    return wrap

@app.route('/api/admin/create_user', methods=['POST'])
@admin_required
def create_user():
    data = request.json
    username = data.get('username', '').strip()
    if not username:
        username = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    else:
        username = username.upper()
    if User.query.filter_by(username=username).first():
        return jsonify({'error': 'Username already exists'}), 400
    balance = float(data.get('balance', 0))
    expires_str = data.get('expires_at')
    expires_at = datetime.fromisoformat(expires_str) if expires_str else None
    user = User(username=username, balance=balance, expires_at=expires_at)
    db.session.add(user)
    db.session.commit()
    return jsonify({'success': True, 'username': username})

@app.route('/api/admin/users', methods=['GET'])
@admin_required
def list_users():
    users = User.query.filter(User.is_admin == False).all()
    return jsonify([{
        'id': u.id, 'username': u.username, 'balance': u.balance,
        'expires_at': u.expires_at.isoformat() if u.expires_at else None
    } for u in users])

@app.route('/api/admin/user_logs/<int:user_id>', methods=['GET'])
@admin_required
def user_logs(user_id):
    logs = StreamLog.query.filter_by(user_id=user_id).order_by(StreamLog.timestamp.desc()).limit(50).all()
    return jsonify([{'title': log.video_title, 'url': log.video_url, 'timestamp': log.timestamp.isoformat()} for log in logs])

@app.route('/api/admin/update_balance', methods=['POST'])
@admin_required
def update_balance():
    data = request.json
    user_id = data.get('user_id')
    days = int(data.get('days', 0))  # positive to add, negative to deduct
    user = User.query.get(user_id)
    if not user or user.is_admin:
        return jsonify({'error': 'User not found'}), 404
    # Adjust balance
    amount = days * 0.36
    if amount >= 0:
        user.balance += amount
    else:
        user.balance = max(0, user.balance + amount)  # amount is negative
    # Auto-adjust expiry
    if days > 0:
        if user.expires_at:
            user.expires_at += timedelta(days=days)
        else:
            user.expires_at = datetime.utcnow() + timedelta(days=days)
    elif days < 0:
        if user.expires_at:
            user.expires_at -= timedelta(days=abs(days))
            if user.expires_at < datetime.utcnow():
                user.expires_at = datetime.utcnow()
    db.session.commit()
    return jsonify({'success': True, 'balance': user.balance, 'expires_at': user.expires_at.isoformat() if user.expires_at else None})

@app.route('/api/admin/delete_user', methods=['POST'])
@admin_required
def delete_user():
    user_id = request.json.get('user_id')
    user = User.query.get(user_id)
    if user and not user.is_admin:
        StreamLog.query.filter_by(user_id=user_id).delete()
        db.session.delete(user)
        db.session.commit()
    return jsonify({'success': True})

@app.route('/api/admin/clear_device', methods=['POST'])
@admin_required
def clear_device_binding():
    data = request.json
    user_id = data.get('user_id')
    user = User.query.get(user_id)
    if user and not user.is_admin:
        user.device_id = None
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'error': 'User not found'}), 404

# -------------------------------
# Broadcast & Notifications
# -------------------------------
@app.route('/api/broadcast/latest')
def get_latest_broadcast():
    data = load_broadcast()
    return jsonify({'message': data.get('message', ''), 'timestamp': data.get('timestamp', 0)})

@app.route('/api/admin/notify', methods=['POST'])
def admin_notify():
    if not (session.get('admin') or (current_user.is_authenticated and current_user.is_admin)):
        return jsonify({'error': 'Admin required'}), 403
    msg = request.json.get('message', '').strip()
    if not msg:
        return jsonify({'error': 'Message cannot be empty'}), 400
    data = load_broadcast()
    data['message'] = msg
    data['timestamp'] = time.time()
    save_broadcast(data)
    return jsonify({'success': True})

# Auth Toggle
@app.route('/api/admin/toggle_auth', methods=['POST'])
def toggle_auth():
    if not (session.get('admin') or (current_user.is_authenticated and current_user.is_admin)):
        return jsonify({'error': 'Admin required'}), 403
    cfg = load_config()
    cfg['auth_required'] = not cfg.get('auth_required', True)
    save_config(cfg)
    return jsonify({'success': True, 'auth_required': cfg['auth_required']})

@app.route('/api/public/auth_status')
def public_auth_status():
    cfg = load_config()
    return jsonify({'auth_required': cfg.get('auth_required', True)})

@app.route('/api/admin/clear-cache', methods=['POST'])
def clear_cache():
    if not (session.get('admin') or (current_user.is_authenticated and current_user.is_admin)):
        return jsonify({'error': 'Admin required'}), 403
    try:
        if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)