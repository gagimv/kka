#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 详情抓取脚本 V2.6 (成功输出 URL + 进度增强)
- 每条保存成功后立刻输出 URL 和番号
- 进度日志附带最近成功的链接
- 高并发稳定，支持断点续抓
"""

import ssl
try:
    import certifi
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except:
    ssl._create_default_https_context = ssl._create_unverified_context

import os, sys, re, json, time, threading, tempfile, shutil, sqlite3, atexit, random, subprocess
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

HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
RESCRAPE = os.environ.get("RESCRAPE", "false").lower() == "true"

class Config:
    MAX_DETAIL_WORKERS = 6
    DB_PATH = "videos.db"
    PROGRESS_FILE = ".processed_urls.txt"
    BACKUP_DIR = "/tmp/missav_backup"
    CLOUDFLARE_MAX_WAIT = 60
    PAGE_LOAD_TIMEOUT = 60
    PAGE_LOAD_WAIT_DETAIL = 6
    M3U8_RETRY_TIMES = 5
    M3U8_RETRY_WAIT_BASE = 2
    DRIVER_MAX_PAGES = 15
    DETAIL_TASK_TIMEOUT = 120

# ---------- 全局锁与缓存 ----------
init_lock = threading.Lock()
save_lock = threading.Lock()
progress_lock = threading.Lock()
print_lock = threading.Lock()  # 防止成功日志交错
thread_local = threading.local()
driver_pool = []

_driver_cache_lock = threading.Lock()
_driver_path_cache = None
_chrome_bin_cache = None
_chrome_version_cache = None

# ---------- Chrome 查找与驱动 ----------
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
        opts.add_argument('--disable-dbus')
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

# ---------- 进度文件 ----------
def load_processed_urls():
    if RESCRAPE:
        return set()
    if not os.path.exists(Config.PROGRESS_FILE):
        return set()
    with open(Config.PROGRESS_FILE, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def mark_url_processed(url):
    with progress_lock:
        with open(Config.PROGRESS_FILE, 'a') as f:
            f.write(url + '\n')

# ---------- 数据库 ----------
def init_database():
    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=5000')
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
                        code TEXT PRIMARY KEY,
                        title TEXT,
                        m3u8 TEXT,
                        cover TEXT,
                        processed INTEGER DEFAULT 0,
                        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_processed ON videos(processed)')
    conn.close()
    migrate_database()

def migrate_database():
    new_cols = {'description':'TEXT','release_date':'TEXT','raw_code':'TEXT',
                'actress':'TEXT','genres':'TEXT','series':'TEXT','studio':'TEXT',
                'director':'TEXT','label':'TEXT'}
    conn = sqlite3.connect(Config.DB_PATH)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(videos)")}
    for col, t in new_cols.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE videos ADD COLUMN {col} {t}")
            except Exception as e:
                print(f"[数据库] 添加列 {col} 失败: {e}")
    conn.commit()
    conn.close()

def is_already_processed(code):
    if RESCRAPE:
        return False
    conn = sqlite3.connect(Config.DB_PATH)
    cur = conn.execute("SELECT m3u8 FROM videos WHERE code=?", (code,))
    row = cur.fetchone()
    conn.close()
    return row is not None and row[0].strip() != ''

def save_video(details):
    code = details.get('code')
    m3u8 = details.get('m3u8', '').strip()
    if not code or not m3u8:
        return False
    with save_lock:
        conn = sqlite3.connect(Config.DB_PATH)
        conn.execute('''INSERT OR REPLACE INTO videos (
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
        conn.commit()
        conn.close()
    return True

# ---------- Driver 实例 ----------
def _create_chrome_instance():
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
        thread_local.driver = _create_chrome_instance()
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
atexit.register(cleanup_drivers)

# ---------- 爬虫类 ----------
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
                print(f"  [GET] 第{i+1}次超时: {e}")
                if i == retries - 1:
                    print(f"  [GET] 连续失败，重置 driver")
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
        details['code'] = m.group(1).upper().replace('-UNCENSORED-LEAK', '')
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

def process_one_detail(url):
    time.sleep(random.uniform(1, 2))
    m = re.search(r'/cn/([^/?#]+)', url, re.I)
    if not m:
        return None  # 无法识别，返回 None 表示失败
    code = m.group(1).upper().replace('-UNCENSORED-LEAK', '')
    if is_already_processed(code):
        return None
    try:
        driver = get_detail_driver()
        scraper = MissAVDetailScraper(driver)
        details = scraper.get_video_details(url)
        if details.get('code') and details.get('m3u8'):
            if save_video(details):
                with print_lock:
                    print(f"✅ {details['code']} 已保存 → {url}")
                return url  # 返回成功处理的 URL
    except Exception as e:
        print(f"  [异常] {url}: {e}")
        reset_detail_driver()
    return None

def main():
    print("="*60)
    print("MissAV 详情抓取 V2.6 (成功日志增强)")
    print(f"无头: {HEADLESS} | 强制重抓: {RESCRAPE}")
    print("="*60)

    init_database()
    processed_urls = load_processed_urls()

    url_file = "urls.txt"
    if not os.path.exists(url_file):
        print(f"错误：未找到 {url_file}")
        sys.exit(1)
    with open(url_file, 'r') as f:
        all_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    if not all_urls:
        print("urls.txt 为空")
        sys.exit(1)

    remaining = [u for u in all_urls if u not in processed_urls]
    print(f"总链接: {len(all_urls)} | 已完成: {len(all_urls)-len(remaining)} | 剩余: {len(remaining)}")
    if not remaining:
        print("所有链接已处理完毕。")
        return

    print("[主线程] 准备 chromedriver...")
    _download_and_patch_driver()
    print("[主线程] 开始并发抓取详情\n")

    total_success = 0
    last_success_url = ""
    start = time.time()

    with ThreadPoolExecutor(max_workers=Config.MAX_DETAIL_WORKERS) as executor:
        futures = {}
        for url in remaining:
            future = executor.submit(process_one_detail, url)
            futures[future] = url
        completed = 0
        for future in as_completed(futures):
            url = futures[future]
            completed += 1
            try:
                result = future.result(timeout=Config.DETAIL_TASK_TIMEOUT)
                if result:  # 返回了成功处理的 URL
                    total_success += 1
                    last_success_url = result
            except TimeoutError:
                print(f"  ⚠ 任务超时: {url}")
            except Exception as e:
                print(f"  [任务异常] {url}: {e}")
            # 无论成功与否，标记已处理，避免死循环
            mark_url_processed(url)
            if completed % 10 == 0:
                elapsed = time.time() - start
                progress_msg = f"  进度: {completed}/{len(remaining)} | 成功: {total_success} | 耗时 {elapsed:.0f}s"
                if last_success_url:
                    progress_msg += f" | 最近: {last_success_url}"
                print(progress_msg)

    cleanup_drivers()
    print(f"\n任务结束。本次成功: {total_success}")

if __name__ == "__main__":
    main()
