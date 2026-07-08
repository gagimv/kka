#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 整合抓取脚本 V3.1（修复判重 & 详情分离）
- 自动抓取 urls.txt 中每个链接的前20页视频列表
- 与 videos.db 对比（仅检查 code 是否存在），只对缺失的番号执行详情抓取
- 详情结果存入 videos_new.db，同时在 videos.db 标记 code 已处理
- 支持断点续抓（列表进度存入 log.db）
- 高并发，环境变量：HEADLESS / RESCRAPE / MAX_LIST_PAGES
- 修复：强制使用脚本所在目录的绝对路径，统一 code 为大写，解决已存在但误判为缺失的问题
"""

import ssl
try:
    import certifi
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except:
    ssl._create_default_https_context = ssl._unverified_context

import os, sys, re, json, time, threading, tempfile, shutil, sqlite3, random, subprocess
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import warnings
warnings.filterwarnings('ignore', category=ResourceWarning)

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
except ImportError:
    print("请先安装 undetected-chromedriver: pip install undetected-chromedriver")
    sys.exit(1)

# ---------- 环境配置 ----------
HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
RESCRAPE = os.environ.get("RESCRAPE", "false").lower() == "true"
MAX_LIST_PAGES = int(os.environ.get("MAX_LIST_PAGES", "20"))

# ---------- 全局配置 ----------
class Config:
    # 使用脚本所在目录的绝对路径，防止工作目录变动导致数据库找不到
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.join(BASE_DIR, "videos.db")           # 仅用于 code 判重
    DETAIL_DB_PATH = os.path.join(BASE_DIR, "videos_new.db")  # 存储完整详情
    LOG_DB_PATH = os.path.join(BASE_DIR, "log.db")          # 列表进度

    MAX_LIST_WORKERS = 6
    PAGE_LOAD_TIMEOUT = 90
    PAGE_LOAD_WAIT_LIST = 5
    CLOUDFLARE_MAX_WAIT = 60
    MAX_DETAIL_WORKERS = 6
    PAGE_LOAD_WAIT_DETAIL = 6
    M3U8_RETRY_TIMES = 5
    M3U8_RETRY_WAIT_BASE = 2
    DRIVER_MAX_PAGES = 15
    DETAIL_TASK_TIMEOUT = 120

# ---------- 全局锁 ----------
init_lock = threading.Lock()
save_lock = threading.Lock()
progress_lock = threading.Lock()
print_lock = threading.Lock()
_driver_cache_lock = threading.Lock()
_driver_path_cache = None
_chrome_bin_cache = None
_chrome_version_cache = None

thread_local = threading.local()
driver_pool = []

# ---------- Chrome 驱动管理 ----------
def _get_chrome_version(browser_path):
    try:
        output = subprocess.check_output([browser_path, '--version'], stderr=subprocess.STDOUT).decode()
        m = re.search(r'(\d+)\.', output)
        return int(m.group(1)) if m else None
    except:
        return None

def _find_chrome_binary():
    chrome_bin = os.environ.get("CHROME_BIN", "")
    if chrome_bin and os.path.isfile(chrome_bin) and os.access(chrome_bin, os.X_OK):
        return chrome_bin
    hard = ["/opt/google/chrome/chrome", "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium-browser", "/usr/local/bin/google-chrome"]
    for p in hard:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    for name in ["google-chrome", "google-chrome-stable", "chrome", "chromium-browser"]:
        p = shutil.which(name)
        if p:
            return p
    print("[错误] 未找到 Chrome 浏览器")
    sys.exit(1)

def _download_and_patch_driver():
    global _driver_path_cache, _chrome_bin_cache, _chrome_version_cache
    with _driver_cache_lock:
        if _driver_path_cache:
            return _driver_path_cache, _chrome_bin_cache
        chrome_bin = _find_chrome_binary()
        ver = _get_chrome_version(chrome_bin)
        if ver:
            print(f"[信息] Chrome 主版本: {ver}")
            _chrome_version_cache = ver
        tmpdir = tempfile.mkdtemp(prefix="missav_init_")
        opts = uc.ChromeOptions()
        opts.binary_location = chrome_bin
        if HEADLESS:
            opts.add_argument('--headless=new')
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--disable-gpu')
        opts.add_argument('--disable-logging')
        opts.add_argument('--window-size=800,600')
        kw = {'options': opts, 'browser_executable_path': chrome_bin, 'use_subprocess': False}
        if ver:
            kw['version_main'] = ver
        print("[信息] 下载/修补 chromedriver...")
        driver = uc.Chrome(**kw)
        dpath = driver.service.path if hasattr(driver, 'service') else None
        if not dpath:
            try:
                dpath = driver.patcher.executable_path
            except:
                pass
        driver.quit()
        shutil.rmtree(tmpdir, ignore_errors=True)
        if not dpath or not os.path.isfile(dpath):
            print("[错误] chromedriver 获取失败")
            sys.exit(1)
        print(f"[信息] chromedriver: {dpath}")
        _driver_path_cache = dpath
        _chrome_bin_cache = chrome_bin
        return dpath, chrome_bin

def _create_list_driver():
    dpath, cbin = _download_and_patch_driver()
    ver = _chrome_version_cache
    tmpdir = tempfile.mkdtemp(prefix="missav_list_")
    opts = uc.ChromeOptions()
    opts.add_argument(f"--user-data-dir={tmpdir}")
    opts.binary_location = cbin
    if HEADLESS:
        opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--disable-logging')
    opts.add_argument('--window-size=800,600')
    kw = {'options': opts, 'browser_executable_path': cbin,
          'driver_executable_path': dpath, 'use_subprocess': False}
    if ver:
        kw['version_main'] = ver
    driver = uc.Chrome(**kw)
    driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
    return driver, tmpdir

def _create_detail_driver():
    dpath, cbin = _download_and_patch_driver()
    ver = _chrome_version_cache
    tmpdir = tempfile.mkdtemp(prefix="missav_detail_")
    opts = uc.ChromeOptions()
    opts.add_argument(f"--user-data-dir={tmpdir}")
    opts.binary_location = cbin
    if HEADLESS:
        opts.add_argument('--headless=new')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--disable-logging')
    opts.add_argument('--disable-dbus')
    opts.add_argument('--disable-background-networking')
    opts.add_argument('--disable-sync')
    opts.add_argument('--disk-cache-size=0')
    opts.add_argument('--disable-extensions')
    opts.add_argument('--disable-images')
    opts.add_argument('--blink-settings=imagesEnabled=false')
    opts.add_argument('--window-size=800,600')
    opts.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    kw = {'options': opts, 'browser_executable_path': cbin,
          'driver_executable_path': dpath, 'use_subprocess': False}
    if ver:
        kw['version_main'] = ver
    with init_lock:
        driver = uc.Chrome(**kw)
    driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
    return driver

def get_detail_driver():
    if hasattr(thread_local, 'driver') and hasattr(thread_local, 'page_count'):
        if thread_local.page_count >= Config.DRIVER_MAX_PAGES:
            reset_detail_driver()
    if not hasattr(thread_local, 'driver'):
        thread_local.driver = _create_detail_driver()
        thread_local.page_count = 0
        driver_pool.append(thread_local.driver)
    try:
        thread_local.driver.get('data:text/html,<html><body>OK</body></html>')
    except:
        reset_detail_driver()
        return get_detail_driver()
    thread_local.page_count += 1
    return thread_local.driver

def reset_detail_driver():
    if hasattr(thread_local, 'driver'):
        try: thread_local.driver.quit()
        except: pass
        delattr(thread_local, 'driver')
    if hasattr(thread_local, 'page_count'):
        delattr(thread_local, 'page_count')

def cleanup_drivers():
    for d in driver_pool:
        try: d.quit()
        except: pass
import atexit
atexit.register(cleanup_drivers)

# ---------- 数据库初始化（修复路径 & 统一大写）----------
def init_databases():
    # 1) 判重数据库 videos.db —— 仅含 code 列，统一为大写
    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
                        code TEXT PRIMARY KEY
                    )''')
    # 将已有数据中的 code 统一转换为大写，避免大小写不一致导致的判重失误
    conn.execute("UPDATE videos SET code = UPPER(code)")
    conn.commit()
    conn.close()

    # 2) 详情数据库 videos_new.db —— 完整字段
    conn = sqlite3.connect(Config.DETAIL_DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
                        code TEXT PRIMARY KEY,
                        title TEXT,
                        m3u8 TEXT,
                        cover TEXT,
                        processed INTEGER DEFAULT 0,
                        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        description TEXT,
                        release_date TEXT,
                        raw_code TEXT,
                        actress TEXT,
                        genres TEXT,
                        series TEXT,
                        studio TEXT,
                        director TEXT,
                        label TEXT
                    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_processed ON videos(processed)')
    conn.commit()
    conn.close()

    # 3) 列表进度库 log.db
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS scrape_progress (
                        source_url TEXT PRIMARY KEY,
                        current_page INTEGER,
                        status TEXT
                    )''')
    conn.commit()
    conn.close()

def get_list_progress(source_url):
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    cur = conn.execute("SELECT current_page, status FROM scrape_progress WHERE source_url=?", (source_url,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None, None

def update_list_progress(source_url, page, status):
    with save_lock:
        conn = sqlite3.connect(Config.LOG_DB_PATH)
        conn.execute('''INSERT OR REPLACE INTO scrape_progress (source_url, current_page, status)
                        VALUES (?, ?, ?)''', (source_url, page, status))
        conn.commit()
        conn.close()

# ---------- 判重函数（修复大小写）----------
def is_video_complete(code):
    """检查 code 是否已存在于 videos.db（不区分大小写，去空格）"""
    clean_code = code.strip().upper()
    if not clean_code:
        return False
    conn = sqlite3.connect(Config.DB_PATH)
    # 使用 UPPER(code) 进行查询，确保大小写不敏感
    cur = conn.execute("SELECT 1 FROM videos WHERE UPPER(code) = ?", (clean_code,))
    row = cur.fetchone()
    conn.close()
    return row is not None

# ---------- 详情保存：写入 videos_new.db，同时标记 videos.db ----------
def save_video(details):
    # 统一 code 为大写
    code = details.get('code', '').strip().upper()
    m3u8 = details.get('m3u8', '').strip()
    if not code or not m3u8:
        return False
    with save_lock:
        # 写入完整详情到 videos_new.db
        conn_new = sqlite3.connect(Config.DETAIL_DB_PATH)
        conn_new.execute('''INSERT OR REPLACE INTO videos (
                                code, title, m3u8, cover, processed,
                                description, release_date, raw_code,
                                actress, genres, series, studio, director, label
                            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (code,
                          details.get('title', ''),
                          m3u8,
                          details.get('cover', ''),
                          details.get('description', ''),
                          details.get('release_date', ''),
                          details.get('raw_code', ''),
                          details.get('actress', ''),
                          details.get('genres', ''),
                          details.get('series', ''),
                          details.get('studio', ''),
                          details.get('director', ''),
                          details.get('label', '')
                         ))
        conn_new.commit()
        conn_new.close()

        # 同时在判重库 videos.db 中插入 code（如果不存在），防止重复抓取
        conn_old = sqlite3.connect(Config.DB_PATH)
        conn_old.execute('INSERT OR IGNORE INTO videos (code) VALUES (?)', (code,))
        conn_old.commit()
        conn_old.close()
    return True

# ---------- 列表抓取类 ----------
class MissAVListScraper:
    def __init__(self, driver):
        self.driver = driver
        self.base_url = "https://missav.ws"

    def wait_for_cloudflare(self, max_wait=Config.CLOUDFLARE_MAX_WAIT):
        start = time.time()
        while time.time() - start < max_wait:
            try:
                if self.driver.find_elements(By.CSS_SELECTOR, 'a[class*="group-hover\\:text-primary"]'):
                    return True
                body = self.driver.find_element(By.TAG_NAME, 'body').text.lower()
                if len(body) > 200 and any(k in body for k in ['missav', '新作上市', 'makers']):
                    return True
                if 'checking your browser' in body or 'just a moment' in body:
                    time.sleep(3)
                    continue
            except:
                pass
            time.sleep(2)
        return False

    def extract_videos_from_page(self):
        videos = {}
        NON_VIDEO = {'genres','actresses','makers','tags','series','studios','labels','rankings','release','search'}
        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, 'a[class*="text-secondary"][class*="group-hover"]')
            if not links:
                links = self.driver.find_elements(By.CSS_SELECTOR, 'a[alt][href*="/cn/"]')
            for link in links:
                try:
                    href = link.get_attribute('href')
                    alt = link.get_attribute('alt')
                    if not href or '/cn/' not in href: continue
                    path = urlparse(href).path.lower().strip('/').split('/')
                    if len(path) >= 2 and path[0] == 'cn' and path[1] in NON_VIDEO: continue
                    code = None
                    if alt and alt.strip():
                        code = alt.strip().upper()
                    else:
                        m = re.search(r'/cn/([^/?#]+)', href, re.I)
                        if m and m.group(1).upper() not in {s.upper() for s in NON_VIDEO}:
                            code = m.group(1).upper()
                    if code and code not in videos:
                        if href.startswith('/'): href = f"{self.base_url}{href}"
                        videos[code] = href
                except: continue
            return list(videos.values())
        except Exception as e:
            print(f"  [提取错误] {e}")
            return []

    def get_total_pages(self):
        try:
            body = self.driver.find_element(By.TAG_NAME, 'body').text
            m = re.search(r'/\s*(\d+)', body)
            if m: return int(m.group(1))
            max_page = 1
            for a in self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="page="]'):
                m = re.search(r'page=(\d+)', a.get_attribute('href'))
                if m: max_page = max(max_page, int(m.group(1)))
            return max_page if max_page > 1 else None
        except: return None

    def safe_get(self, url, retries=5):
        for i in range(retries):
            try:
                self.driver.get(url)
                return True
            except Exception as e:
                print(f"  [GET] 第{i+1}次失败: {e}")
                if i == retries - 1:
                    print(f"  [GET] 连续超时")
                    return False
                time.sleep(5 * (i + 1))
        return False

    def fetch_video_entries(self, url, start_page=1, max_pages=None):
        entries = []
        print(f"  [列表] 开始 {url}（起始页 {start_page}，最多 {max_pages} 页）")
        base = url.rstrip('/')
        current_page = start_page
        try:
            if start_page > 1:
                first_url = f"{base}{'&' if '?' in base else '?'}page={start_page}"
                if not self.safe_get(first_url):
                    return entries, start_page
            else:
                if not self.safe_get(base):
                    return entries, start_page
            if not self.wait_for_cloudflare():
                return entries, start_page
            time.sleep(Config.PAGE_LOAD_WAIT_LIST)

            total = self.get_total_pages()
            if max_pages and total:
                total = min(total, start_page + max_pages - 1)
            elif max_pages:
                total = start_page + max_pages - 1

            while True:
                if current_page > start_page:
                    page_url = f"{base}{'&' if '?' in base else '?'}page={current_page}"
                    print(f"  [分页] 第{current_page}页 -> {page_url}")
                    if not self.safe_get(page_url) or not self.wait_for_cloudflare():
                        break
                else:
                    print(f"  [分页] 第{current_page}页")

                try:
                    if '未有记录' in self.driver.find_element(By.TAG_NAME, 'body').text:
                        print("  [停止] 无数据")
                        break
                except: pass

                videos = self.extract_videos_from_page()
                if not videos:
                    self.driver.refresh()
                    time.sleep(5)
                    if not self.wait_for_cloudflare(): break
                    videos = self.extract_videos_from_page()
                    if not videos: break
                print(f"  -> 本页 {len(videos)} 部")

                for v in videos:
                    m = re.search(r'/cn/([^/?#]+)', v, re.I)
                    if m:
                        # 提取并统一为大写，去除 -UNCENSORED-LEAK 后缀
                        code = m.group(1).upper().replace('-UNCENSORED-LEAK', '').strip()
                        entries.append((code, v))

                update_list_progress(url, current_page, 'in_progress')

                if total and current_page >= total:
                    break
                has_next = False
                for a in self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="page="]'):
                    m = re.search(r'page=(\d+)', a.get_attribute('href'))
                    if m and int(m.group(1)) > current_page:
                        has_next = True
                        break
                if not has_next and not total:
                    break
                current_page += 1
                time.sleep(3)

            update_list_progress(url, current_page, 'completed')
            print(f"  [列表完成] 共 {len(entries)} 部")
        except Exception as e:
            print(f"  [列表错误] {e}")
        return entries, current_page

# ---------- 详情抓取类 ----------
class MissAVDetailScraper:
    def __init__(self, driver):
        self.driver = driver

    def wait_for_cloudflare(self, max_wait=Config.CLOUDFLARE_MAX_WAIT):
        start = time.time()
        while time.time() - start < max_wait:
            try:
                if self.driver.find_elements(By.CSS_SELECTOR, 'a[class*="group-hover\\:text-primary"]'):
                    return True
                body = self.driver.find_element(By.TAG_NAME, 'body').text.lower()
                if len(body) > 200 and any(k in body for k in ['missav', '新作上市']):
                    return True
                if 'checking your browser' in body or 'just a moment' in body:
                    time.sleep(3)
                    continue
            except:
                pass
            time.sleep(2)
        return False

    def safe_get(self, url, retries=5):
        for i in range(retries):
            try:
                self.driver.get(url)
                return True
            except Exception as e:
                print(f"  [GET详情] 第{i+1}次失败: {e}")
                if i == retries - 1:
                    print(f"  [GET详情] 连续失败，重置 driver")
                    reset_detail_driver()
                else:
                    time.sleep(5 * (i + 1))
        return False

    def _extract_detail_fields(self):
        fields = {'description':'','release_date':'','raw_code':'','title':'',
                  'actress':'','genres':'','series':'','studio':'','director':'','label':''}
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@x-show=\"currentTab === 'video_details'\"]"))
            )
            div = self.driver.find_element(By.XPATH, "//div[@x-show=\"currentTab === 'video_details'\"]")
            try:
                d = div.find_element(By.XPATH, ".//div[contains(@class,'line-clamp')]")
                fields['description'] = d.text.strip()
            except: pass
            try:
                t = div.find_element(By.XPATH, ".//time")
                fields['release_date'] = t.get_attribute('datetime') or t.text.strip()
            except: pass
            for field, xp in [('raw_code',".//span[text()='番号:']/following-sibling::span"),
                              ('title',".//span[text()='标题:']/following-sibling::span")]:
                try:
                    fields[field] = div.find_element(By.XPATH, xp).text.strip()
                except: pass
            for field, label in [('actress','女优'),('genres','类型'),('series','系列'),
                                 ('studio','发行商'),('director','导演'),('label','标籤')]:
                try:
                    elems = div.find_elements(By.XPATH, f".//span[text()='{label}:']/following-sibling::a")
                    fields[field] = ', '.join([e.text.strip() for e in elems if e.text.strip()])
                except: pass
        except Exception as e:
            print(f"  [元数据] 异常: {e}")
        return fields

    def get_video_details(self, video_url):
        details = {'url':video_url, 'code':'', 'cover':'', 'm3u8':''}
        for k in ['description','release_date','raw_code','title','actress','genres','series','studio','director','label']:
            details[k] = ''
        m = re.search(r'/cn/([^/?#]+)', video_url, re.I)
        if not m:
            return details
        details['code'] = m.group(1).upper().replace('-UNCENSORED-LEAK', '').strip()
        if not self.safe_get(video_url) or not self.wait_for_cloudflare():
            return details
        time.sleep(Config.PAGE_LOAD_WAIT_DETAIL)
        try:
            h1 = self.driver.find_element(By.TAG_NAME, 'h1')
            if h1.text.strip() != 'missav.ws':
                details['title'] = h1.text.strip()
        except: pass
        try:
            og = self.driver.find_element(By.CSS_SELECTOR, 'meta[property="og:image"]')
            details['cover'] = og.get_attribute('content')
        except:
            if details['code']:
                details['cover'] = f"https://fourhoi.com/{details['code'].lower()}/cover-n.jpg"
        meta = self._extract_detail_fields()
        details.update(meta)
        if not details.get('title'):
            details['title'] = meta.get('title', '')
        for attempt in range(1, Config.M3U8_RETRY_TIMES + 1):
            m3u8 = ""
            try:
                for s in self.driver.find_elements(By.TAG_NAME, 'script'):
                    content = s.get_attribute('outerHTML') or s.get_attribute('innerHTML') or ''
                    if 'm3u8' in content:
                        found = re.findall(r'(https?://[^\s\'\"<>]+\.m3u8[^\s\'\"<>]*)', content)
                        if found:
                            m3u8 = found[0]
                            break
            except: pass
            if not m3u8:
                try:
                    for v in self.driver.find_elements(By.TAG_NAME, 'video'):
                        src = v.get_attribute('src')
                        if src and '.m3u8' in src:
                            m3u8 = src
                            break
                except: pass
            if not m3u8:
                try:
                    logs = self.driver.get_log('performance')
                    for entry in logs:
                        msg = json.loads(entry['message'])['message']
                        if msg.get('method') == 'Network.responseReceived':
                            u = msg.get('params',{}).get('response',{}).get('url','')
                            if '.m3u8' in u:
                                m3u8 = u
                                break
                except: pass
            if m3u8:
                details['m3u8'] = m3u8.strip()
                break
            else:
                if attempt < Config.M3U8_RETRY_TIMES:
                    wait = min(attempt * Config.M3U8_RETRY_WAIT_BASE, 30)
                    print(f"  ⚠ m3u8 第{attempt}次未找到，等待{wait}s刷新")
                    time.sleep(wait)
                    self.driver.refresh()
                    if not self.wait_for_cloudflare():
                        if not self.safe_get(video_url):
                            break
                    else:
                        time.sleep(Config.PAGE_LOAD_WAIT_DETAIL)
                else:
                    print(f"  ✗ m3u8 重试{Config.M3U8_RETRY_TIMES}次失败")
                    details['m3u8'] = ''
                    reset_detail_driver()
        return details

# ---------- 任务处理 ----------
def collect_list_entries(url, start_page, max_pages):
    driver, tmp_dir = _create_list_driver()
    try:
        scraper = MissAVListScraper(driver)
        entries, last_page = scraper.fetch_video_entries(url, start_page, max_pages)
        return entries, last_page
    except Exception as e:
        print(f"[列表异常] {url}: {e}")
        return [], start_page
    finally:
        try: driver.quit()
        except: pass
        shutil.rmtree(tmp_dir, ignore_errors=True)

def process_one_detail(code, url):
    # 再次判重（并发安全）
    if is_video_complete(code):
        return None
    time.sleep(random.uniform(1, 2))
    try:
        driver = get_detail_driver()
        scraper = MissAVDetailScraper(driver)
        details = scraper.get_video_details(url)
        if details.get('code') and details.get('m3u8'):
            if save_video(details):
                with print_lock:
                    print(f"✅ {details['code']} 已保存 → {url}")
                return code
    except Exception as e:
        print(f"  [详情异常] {url}: {e}")
        reset_detail_driver()
    return None

# ---------- 主流程 ----------
def main():
    print("="*60)
    print("MissAV 整合抓取脚本 V3.1")
    print(f"无头: {HEADLESS} | 强制重抓: {RESCRAPE} | 列表最大页数: {MAX_LIST_PAGES}")
    print(f"数据库目录: {Config.BASE_DIR}")
    print("="*60)

    init_databases()

    url_file = "urls.txt"
    if not os.path.exists(url_file):
        print(f"错误：未找到 {url_file}")
        sys.exit(1)

    with open(url_file, 'r', encoding='utf-8') as f:
        all_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not all_urls:
        print(f"错误：{url_file} 中没有有效 URL")
        sys.exit(1)

    list_tasks = []
    skip_count = 0
    for url in all_urls:
        if not RESCRAPE:
            last_page, status = get_list_progress(url)
            if status == 'completed':
                skip_count += 1
                continue
            start_page = last_page + 1 if status == 'in_progress' else 1
        else:
            start_page = 1
        list_tasks.append((url, start_page, MAX_LIST_PAGES))

    print(f"从 {url_file} 读取到 {len(all_urls)} 个链接，跳过已完成 {skip_count}，待处理列表 {len(list_tasks)}")

    all_entries = []
    if list_tasks:
        print("[主线程] 准备 chromedriver...")
        _download_and_patch_driver()
        print("[主线程] 开始并发抓取列表页\n")
        with ThreadPoolExecutor(max_workers=Config.MAX_LIST_WORKERS) as executor:
            future_to_url = {executor.submit(collect_list_entries, url, sp, mp): url for url, sp, mp in list_tasks}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    entries, last_page = future.result()
                    if entries:
                        all_entries.extend(entries)
                except Exception as e:
                    print(f"任务异常 [{url}]: {e}")

        seen = {}
        for code, url in all_entries:
            if code not in seen:
                seen[code] = url
        unique_entries = [(code, seen[code]) for code in seen]
        print(f"\n列表阶段共收集到 {len(unique_entries)} 个不重复番号。")
    else:
        print("所有列表已完成，跳过列表抓取阶段。")
        unique_entries = []

    # 过滤：仅保留未在 videos.db 中出现的 code
    need_detail = []
    for code, url in unique_entries:
        if not is_video_complete(code):
            need_detail.append((code, url))

    print(f"需要抓取详情的番号: {len(need_detail)} 个")

    if not need_detail:
        print("所有番号已存在，任务结束。")
        return

    print("[主线程] 开始并发抓取详情\n")
    detail_start = time.time()
    total_success = 0
    last_success = ""
    completed_cnt = 0

    with ThreadPoolExecutor(max_workers=Config.MAX_DETAIL_WORKERS) as executor:
        futures = {}
        for code, url in need_detail:
            future = executor.submit(process_one_detail, code, url)
            futures[future] = (code, url)

        for future in as_completed(futures):
            code, url = futures[future]
            completed_cnt += 1
            try:
                result = future.result(timeout=Config.DETAIL_TASK_TIMEOUT)
                if result:
                    total_success += 1
                    last_success = url
            except TimeoutError:
                print(f"  ⚠ 任务超时: {url}")
            except Exception as e:
                print(f"  [任务异常] {url}: {e}")

            if completed_cnt % 10 == 0:
                elapsed = time.time() - detail_start
                msg = f"  进度: {completed_cnt}/{len(need_detail)} | 成功: {total_success} | 耗时 {elapsed:.0f}s"
                if last_success:
                    msg += f" | 最近: {last_success}"
                print(msg)

    cleanup_drivers()
    print(f"\n任务结束。详情阶段成功: {total_success}")

if __name__ == "__main__":
    main()
