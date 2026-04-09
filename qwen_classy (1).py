"""
qwen_classy.py — Render background worker
Configure via environment variables (see CONFIG section).
"""

import asyncio, io, json, logging, math, os, random, signal
import tempfile, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path

import oss2
import psutil
from PIL import Image
from playwright.async_api import async_playwright
from supabase import create_client, Client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-14s] %(levelname)s — %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger('qwen_classy')

# ── Config (set these as Render environment variables) ────────────────────────
SUPABASE_URL   = os.environ['SUPABASE_URL']        # required
SUPABASE_KEY   = os.environ['SUPABASE_KEY']        # required
SUPABASE_TABLE = os.environ.get('SUPABASE_TABLE', 'qwen_tokens')

NUM_WORKERS             = 1        # single worker — lean on Render
PW_CONTEXT_ROTATE_AFTER = 1
PW_PAGE_TIMEOUT         = 30_000
PW_SESSION_TTL          = 400
SUPABASE_BATCH_SIZE         = 50
SUPABASE_FLUSH_EVERY_N_SECS = 15
STATUS_PRINT_EVERY_N_SECS   = 30
MONITOR_EVERY_N_SECS        = 60   # system metrics interval

PW_PLUS_BTN_XPATH  = (
    '/html/body/div[1]/div/div/div[2]/div/div/div'
    '/div/div[1]/div[2]/div/div/div[2]/div/div/div[1]/span/div'
)
PW_UPLOAD_LI_XPATH = '/html/body/div[3]/div/ul/li[1]/span/div'

PNG_TMP_DIR = Path(tempfile.mkdtemp(prefix='qwen_pngs_'))

# ── Helpers ───────────────────────────────────────────────────────────────────
_STS_REQUIRED = {
    'file_id', 'file_url', 'file_path',
    'access_key_id', 'access_key_secret', 'security_token',
    'bucketname', 'region', 'endpoint',
}

def make_png(worker_id: int) -> str:
    img   = Image.new('RGB', (2, 2), color=(
        random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    fpath = PNG_TMP_DIR / f'w{worker_id:02d}_{uuid.uuid4().hex[:8]}.png'
    img.save(str(fpath), format='PNG')
    return str(fpath)

def _validate_sts(data: dict) -> bool:
    return _STS_REQUIRED.issubset(data.keys())

def _backoff(attempt: int) -> float:
    base = [3, 8, 15, 30, 60][min(attempt, 4)]
    return base + random.uniform(0, base * 0.25)

# ── Monitor ───────────────────────────────────────────────────────────────────
_proc      = psutil.Process(os.getpid())
_net_prev  : dict = {}
_disk_prev : dict = {}

def _fmt_bytes(n: float) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if abs(n) < 1024:
            return f'{n:6.1f} {unit}'
        n /= 1024
    return f'{n:6.1f} TB'

def _bar(pct: float, width: int = 10) -> str:
    filled = round(pct / 100 * width)
    return f"{'█'*filled}{'░'*(width-filled)} {pct:5.1f}%"

def _col(title: str, rows: list, width: int = 28) -> list:
    return [f'{title:─<{width}}'] + [f'{r:<{width}}' for r in rows]

def _net_metrics() -> dict:
    c = psutil.net_io_counters(); now = time.time()
    sent_s = recv_s = 0.0
    if _net_prev:
        dt = max(now - _net_prev['t'], 0.001)
        sent_s = (c.bytes_sent - _net_prev['sent']) / dt
        recv_s = (c.bytes_recv - _net_prev['recv']) / dt
    _net_prev.update(t=now, sent=c.bytes_sent, recv=c.bytes_recv)
    return dict(sent_s=sent_s, recv_s=recv_s,
                total_sent=c.bytes_sent, total_recv=c.bytes_recv)

def _disk_metrics() -> dict:
    c = psutil.disk_io_counters(); now = time.time()
    read_s = write_s = 0.0
    if _disk_prev and c:
        dt = max(now - _disk_prev['t'], 0.001)
        read_s  = (c.read_bytes  - _disk_prev['read'])  / dt
        write_s = (c.write_bytes - _disk_prev['write']) / dt
    if c:
        _disk_prev.update(t=now, read=c.read_bytes, write=c.write_bytes)
    return dict(read_s=read_s, write_s=write_s)

def print_monitor() -> None:
    per_core = psutil.cpu_percent(interval=0.5, percpu=True)
    overall  = sum(per_core) / len(per_core)
    mem      = psutil.virtual_memory().percent
    rms      = math.sqrt(sum(x**2 for x in per_core) / len(per_core))
    workload = round(min(rms, 100), 1)
    net      = _net_metrics()
    disk     = _disk_metrics()
    mi       = _proc.memory_info()
    W        = 30

    col_cpu  = _col('CPU / Memory', [
        f"Overall  {_bar(round(overall,1))}",
        f"Memory   {_bar(mem)}",
        f"Workload {_bar(workload)}",
        f"Cores    {len(per_core)}",
    ], W)
    col_net  = _col('Network I/O', [
        f"▲ Sent   {_fmt_bytes(net['sent_s'])}/s",
        f"▼ Recv   {_fmt_bytes(net['recv_s'])}/s",
        f"∑ Sent   {_fmt_bytes(net['total_sent'])}",
        f"∑ Recv   {_fmt_bytes(net['total_recv'])}",
    ], W)
    col_disk = _col('Disk I/O', [
        f"Read     {_fmt_bytes(disk['read_s'])}/s",
        f"Write    {_fmt_bytes(disk['write_s'])}/s",
    ], W)
    col_proc = _col('Process', [
        f"RSS      {_fmt_bytes(mi.rss)}",
        f"VMS      {_fmt_bytes(mi.vms)}",
        f"Threads  {_proc.num_threads()}",
        f"PID      {os.getpid()}",
    ], W)

    cols = [col_cpu, col_net, col_disk, col_proc]
    max_rows = max(len(c) for c in cols)
    for c in cols:
        c += [''] * (max_rows - len(c))
    print()
    for row in zip(*cols):
        print('  ' + '  '.join(f'{cell:<{W}}' for cell in row))
    print()

class SystemMonitor(threading.Thread):
    def __init__(self, stop_event: threading.Event):
        super().__init__(daemon=True, name='SysMonitor')
        self.stop_event = stop_event

    def run(self):
        _net_metrics(); _disk_metrics()   # warm up counters
        time.sleep(1)
        while not self.stop_event.wait(MONITOR_EVERY_N_SECS):
            print_monitor()

# ── BxTokenPool ───────────────────────────────────────────────────────────────
class BxTokenPool:
    """Thread-safe pool. Worker ADDs tokens; flusher DRAINs them."""

    def __init__(self):
        self._lock = threading.Lock()
        self._pool: dict[str, dict] = {}
        self._seen: set[str]        = set()

    def add(self, bx_ua: str, bx_umidtoken: str, bx_v: str):
        if not bx_umidtoken:
            return
        with self._lock:
            if bx_umidtoken in self._pool or bx_umidtoken in self._seen:
                return
            self._pool[bx_umidtoken] = {
                'bx-ua':          bx_ua,
                'bx-umidtoken':   bx_umidtoken,
                'bx-v':           bx_v,
                'generated_time': datetime.now(timezone.utc).isoformat(),
            }
            log.info(f'[BxPool] ➕ ...{bx_umidtoken[-14:]} (pool={len(self._pool)})')

    def drain(self, max_count: int) -> list[dict]:
        rows = []
        with self._lock:
            for k in list(self._pool.keys())[:max_count]:
                tok = self._pool.pop(k)
                self._seen.add(k)
                rows.append({
                    'bx_ua':          tok['bx-ua'],
                    'bx_umidtoken':   tok['bx-umidtoken'],
                    'generated_time': tok['generated_time'],
                })
        return rows

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._pool)

    def status(self) -> str:
        with self._lock:
            return f'pool={len(self._pool)}'

# ── _QwenSession ──────────────────────────────────────────────────────────────
class _QwenSession:
    def __init__(self, pw, bx_pool: BxTokenPool):
        self._pw             = pw
        self._bx_pool        = bx_pool
        self._browser        = None
        self._ctx            = None
        self._page           = None
        self._last_used      = 0.0
        self._uploads_in_ctx = 0

    async def _launch(self):
        self._browser = await self._pw.firefox.launch(headless=True)
        log.info('[PW] 🦊 Firefox launched')

    async def _open_context(self):
        if self._ctx:
            try: await self._ctx.close()
            except Exception: pass
            log.info(f'[PW] ♻️  Context rotated | {self._bx_pool.status()}')
        self._ctx  = await self._browser.new_context(accept_downloads=True)
        self._page = await self._ctx.new_page()
        self._page.on('pageerror', lambda e: log.debug(f'[PW] pageerror: {e}'))
        await self._page.goto(
            'https://chat.qwen.ai',
            wait_until='networkidle',
            timeout=PW_PAGE_TIMEOUT * 3,
        )
        await self._page.wait_for_timeout(1500)
        self._last_used      = time.time()
        self._uploads_in_ctx = 0
        log.info('[PW] 🌐 Fresh context ready')

    async def _is_alive(self) -> bool:
        try:
            if self._page is None or self._page.is_closed():
                return False
            await self._page.evaluate('() => document.readyState')
            return True
        except Exception:
            return False

    async def _heal(self):
        stale = (time.time() - self._last_used) > PW_SESSION_TTL
        if not await self._is_alive() or stale:
            log.warning('[PW] ⚕️  Healing → rotating')
            await self._open_context()

    async def start(self):
        await self._launch()
        await self._open_context()

    async def get_sts_token(self) -> dict:
        if self._uploads_in_ctx >= PW_CONTEXT_ROTATE_AFTER:
            await self._open_context()
        await self._heal()

        file_path   = make_png(0)
        fname       = os.path.basename(file_path)
        sts_result  : dict = {}
        done_event  = asyncio.Event()
        pending_req : dict = {}

        def on_request(req):
            if '/api/v2/files/getstsToken' in req.url and req.method == 'POST':
                try:
                    body         = json.loads(req.post_data or '{}')
                    req_id       = req.headers.get('x-request-id', fname)
                    pending_req[req_id] = body.get('filename', fname)
                    bx_ua        = req.headers.get('bx-ua', '')
                    bx_umidtoken = req.headers.get('bx-umidtoken', '')
                    bx_v         = req.headers.get('bx-v', '2.5.36')
                    if bx_umidtoken:
                        self._bx_pool.add(bx_ua, bx_umidtoken, bx_v)
                except Exception:
                    pass

        async def on_response(resp):
            if '/api/v2/files/getstsToken' not in resp.url:
                return
            try:
                body   = await resp.json()
                req_id = resp.request.headers.get('x-request-id', '')
                _fn    = pending_req.get(req_id, fname)
                if not body.get('success'):
                    code = body.get('data', {}).get('code', '')
                    sts_result['rate_limited' if code == 'RateLimited' else 'error'] = True
                else:
                    data = body.get('data', {})
                    if _validate_sts(data):
                        sts_result.update(data)
                        log.info(f'[PW] 📥 STS captured for {_fn}')
                    else:
                        sts_result['error'] = f'missing: {_STS_REQUIRED - set(data)}'
            except Exception as e:
                sts_result['error'] = str(e)
            finally:
                done_event.set()

        self._page.on('request',  on_request)
        self._page.on('response', on_response)
        try:
            await self._page.wait_for_selector(
                f'xpath={PW_PLUS_BTN_XPATH}', timeout=PW_PAGE_TIMEOUT)
            await self._page.click(f'xpath={PW_PLUS_BTN_XPATH}')
            await self._page.wait_for_timeout(500)
            await self._page.wait_for_selector(
                f'xpath={PW_UPLOAD_LI_XPATH}', timeout=5_000)
            async with self._page.expect_file_chooser(timeout=5_000) as fc_ctx:
                await self._page.click(f'xpath={PW_UPLOAD_LI_XPATH}')
            fc = await fc_ctx.value
            await fc.set_files([file_path])
            log.info(f'[PW] 📤 File set: {fname}')
            try:
                await asyncio.wait_for(done_event.wait(), timeout=45)
            except asyncio.TimeoutError:
                raise TimeoutError(f'No STS response within 45s ({fname})')
            if sts_result.get('rate_limited'):
                raise RuntimeError('rate_limited')
            if 'error' in sts_result:
                raise RuntimeError(f'STS error: {sts_result["error"]}')
            if not _validate_sts(sts_result):
                raise RuntimeError('STS missing keys')
            self._last_used      = time.time()
            self._uploads_in_ctx += 1
            return dict(sts_result)
        finally:
            try: os.unlink(file_path)
            except Exception: pass
            self._page.remove_listener('request',  on_request)
            self._page.remove_listener('response', on_response)

    async def rotate_context(self):
        await self._open_context()

    async def close(self):
        for obj in (self._ctx, self._browser):
            try:
                if obj: await obj.close()
            except Exception:
                pass
        log.info('[PW] 🔒 Browser closed')

# ── PlaywrightWorker ──────────────────────────────────────────────────────────
class PlaywrightWorker(threading.Thread):
    def __init__(self, stop_event: threading.Event, bx_pool: BxTokenPool):
        super().__init__(daemon=True, name='PW00')
        self.stop_event = stop_event
        self.bx_pool    = bx_pool
        self.stats      = dict(success=0, rate_limited=0, timeout=0, errors=0)

    def run(self):
        asyncio.run(self._main())

    async def _main(self):
        log.info('[PW] ▶ started')
        async with async_playwright() as pw:
            session = _QwenSession(pw, self.bx_pool)
            try:
                await session.start()
                await self._loop(session)
            except Exception as e:
                log.error(f'[PW] ❌ Fatal: {e}')
                self.stats['errors'] += 1
            finally:
                await session.close()
        log.info(f'[PW] ⏹ stopped | {self.stats}')

    async def _loop(self, session):
        attempt = 0
        while not self.stop_event.is_set():
            try:
                sts = await session.get_sts_token()
                await asyncio.get_event_loop().run_in_executor(
                    None, self._oss_upload, sts)
                log.info(f'[PW] ✅ OSS OK file_id={sts["file_id"]} | {self.bx_pool.status()}')
                self.stats['success'] += 1
                attempt = 0
                await asyncio.sleep(0.5)

            except RuntimeError as e:
                if 'rate_limited' in str(e):
                    self.stats['rate_limited'] += 1
                    log.warning('[PW] 🚫 Rate limited — rotating')
                    await session.rotate_context()
                    await asyncio.sleep(_backoff(attempt))
                else:
                    await self._err(e, session, attempt)
                attempt += 1

            except TimeoutError as e:
                self.stats['timeout'] += 1
                log.warning('[PW] ⏱️  Timeout — rotating')
                await session.rotate_context()
                await asyncio.sleep(_backoff(attempt))
                attempt += 1

            except Exception as e:
                await self._err(e, session, attempt)
                attempt += 1

    async def _err(self, exc, session, attempt):
        wait = _backoff(attempt)
        log.warning(f'[PW] ⚠️  {exc.__class__.__name__}: {exc} — heal in {wait:.0f}s')
        self.stats['errors'] += 1
        await asyncio.sleep(wait)
        session._last_used = 0
        try: await session._heal()
        except Exception as he: log.warning(f'[PW] Heal failed: {he}')

    @staticmethod
    def _oss_upload(sts: dict):
        buf = io.BytesIO()
        Image.new('RGB', (2, 2), color=(
            random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        ).save(buf, format='PNG')
        auth   = oss2.StsAuth(sts['access_key_id'], sts['access_key_secret'], sts['security_token'])
        bucket = oss2.Bucket(auth, f"https://{sts['endpoint']}", sts['bucketname'])
        res    = bucket.put_object(sts['file_path'], buf.getvalue())
        if res.status != 200:
            raise RuntimeError(f'OSS HTTP {res.status}')

# ── SupabaseFlusher ───────────────────────────────────────────────────────────
class SupabaseFlusher(threading.Thread):
    _ERROR_BACKOFF = [30, 60, 120]

    def __init__(self, bx_pool: BxTokenPool, stop_event: threading.Event):
        super().__init__(daemon=True, name='Flusher')
        self.bx_pool    = bx_pool
        self.stop_event = stop_event
        self._sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.stats = dict(inserted=0, batches=0, errors=0)
        self._consec_errors = 0

    def run(self):
        log.info('[Flusher] ▶ started')
        pending: list[dict] = []
        last_flush = time.time()

        while not self.stop_event.is_set() or pending or self.bx_pool.size > 0:
            pending.extend(self.bx_pool.drain(SUPABASE_BATCH_SIZE))
            now = time.time()
            should_flush = (
                len(pending) >= SUPABASE_BATCH_SIZE
                or (now - last_flush >= SUPABASE_FLUSH_EVERY_N_SECS and pending)
                or (self.stop_event.is_set() and pending)
            )
            if should_flush:
                batch = pending[:SUPABASE_BATCH_SIZE]
                if self._flush(batch):
                    pending    = pending[SUPABASE_BATCH_SIZE:]
                    last_flush = time.time()
                    self._consec_errors = 0
                else:
                    wait = self._ERROR_BACKOFF[min(self._consec_errors, 2)]
                    log.warning(f'[Flusher] ⏳ Backoff {wait}s — {len(pending)} rows pending')
                    self._consec_errors += 1
                    time.sleep(wait)
                    last_flush = time.time()
            else:
                time.sleep(1)

        if pending:
            self._flush(pending)
        log.info(f'[Flusher] ⏹ stopped | {self.stats}')

    def _flush(self, rows: list[dict]) -> bool:
        try:
            self._sb.table(SUPABASE_TABLE) \
                    .upsert(rows, on_conflict='bx_umidtoken') \
                    .execute()
            self.stats['inserted'] += len(rows)
            self.stats['batches']  += 1
            log.info(f'[Flusher] 💾 Batch #{self.stats["batches"]} — +{len(rows)} rows '
                     f'(total={self.stats["inserted"]})')
            return True
        except Exception as e:
            self.stats['errors'] += 1
            log.error(f'[Flusher] ❌ Insert failed: {e}')
            return False

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    stop_event = threading.Event()

    # Render sends SIGTERM on shutdown — handle it cleanly
    def _handle_signal(sig, _frame):
        log.info(f'Signal {sig} received — stopping...')
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    bx_pool = BxTokenPool()
    worker  = PlaywrightWorker(stop_event, bx_pool)
    flusher = SupabaseFlusher(bx_pool, stop_event)
    monitor = SystemMonitor(stop_event)

    worker.start()
    flusher.start()
    monitor.start()

    log.info('🚀 qwen_classy running — worker · flusher · monitor all up')

    start_time = time.time()
    while not stop_event.is_set():
        time.sleep(STATUS_PRINT_EVERY_N_SECS)
        uptime  = int(time.time() - start_time)
        h, m    = divmod(uptime // 60, 60)
        s       = worker.stats
        log.info(
            f'uptime={h:02d}h{m:02d}m | {bx_pool.status()} | '
            f'ok={s["success"]} rl={s["rate_limited"]} '
            f'timeout={s["timeout"]} err={s["errors"]} | '
            f'inserted={flusher.stats["inserted"]} '
            f'batches={flusher.stats["batches"]}'
        )

    worker.join(timeout=30)
    flusher.join(timeout=30)
    log.info(f'✅ Stopped. Inserted={flusher.stats["inserted"]} '
             f'batches={flusher.stats["batches"]} errors={flusher.stats["errors"]}')

if __name__ == '__main__':
    main()
