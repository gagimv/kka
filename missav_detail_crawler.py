#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 详情抓取脚本 V1.0
从 log.db 中读取待处理条目，获取 m3u8、封面链接，并可生成 .nfo 文件
"""

import ssl
try:
    import certifi
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except:
    ssl._create_default_https_context = ssl._create_unverified_context

import os, sys, re, json, time, threading, tempfile, shutil, sqlite3, atexit, random
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import warnings
warnings.filterwarnings('ignore', category=ResourceWarning)

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
except ImportError:
    print("请先安装 undetected-chromedriver: pip install undetected-chromedriver")
    sys.exit(1)

HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
NFO_DIR = os.environ.get("NFO_DIR", "").strip()

class Config:
    MAX_DETAIL_WORKERS = 12
    DB_PATH = "videos.db"
    LOG_DB_PATH = "log.db"
    COMP_DB_PATH = "comp.db"
    BACKUP_DIR = "/tmp/missav_backup"
    CLOUDFLARE_MAX_WAIT = 60
    PAGE_LOAD_TIMEOUT = 90
    PAGE_LOAD_WAIT_DETAIL = 8
    M3U8_RETRY_TIMES = 10
    M3U8_RETRY_WAIT_BASE = 2
    DRIVER_MAX_PAGES = 20
    DETAIL_TASK_TIMEOUT = 120

CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH")
CHROME_BIN = os.environ.get("CHROME_BIN")

init_lock = threading.Lock()
save_lock = threading.Lock()
thread_local = threading.local()
driver_pool = []

# ---------- 备份 ----------
def backup_databases():
    os.makedirs(Config.BACKUP_DIR, exist_ok=True)
    for db_path in [Config.DB_PATH, Config.LOG_DB_PATH]:
        if os.path.exists(db_path):
            try:
                shutil.copy2(db_path, os.path.join(Config.BACKUP_DIR, os.path.basename(db_path)))
            except Exception as e:
                print(f"  ⚠ 备份 {db_path} 失败: {e}")

# ---------- 数据库 ----------
def init_databases():
    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
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

    conn = sqlite3.connect(Config.LOG_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS pending (
                        code TEXT PRIMARY KEY,
                        url TEXT,
                        source_url TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.close()

def get_pending_log():
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    cur = conn.execute("SELECT code, url, source_url FROM pending")
    rows = cur.fetchall()
    conn.close()
    return rows

def remove_from_log(code):
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    with save_lock:
        conn.execute("DELETE FROM pending WHERE code = ?", (code,))
        conn.commit()
    conn.close()

def save_video(details):
    code = details.get('code')
    m3u8 = details.get('m3u8', '').strip()
    if not code or not m3u8:
        return False
    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute('''INSERT OR REPLACE INTO videos (code, title, m3u8, cover, processed)
                    VALUES (?, ?, ?, ?, 1)''',
                 (code, details.get('title', ''), m3u8, details.get('cover', '')))
    conn.commit()
    conn.close()
    remove_from_log(code)

    # 可选：生成 .nfo 文件
    if NFO_DIR:
        try:
            os.makedirs(NFO_DIR, exist_ok=True)
            nfo_path = os.path.join(NFO_DIR, f"{code}.nfo")
            nfo_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<movie>
  <title>{details.get('title', code)}</title>
  <uniqueid type="code">{code}</uniqueid>
  <thumb>{details.get('cover', '')}</thumb>
</movie>
"""
            with open(nfo_path, 'w', encoding='utf-8') as f:
                f.write(nfo_content)
            print(f"  [NFO] 已生成 {nfo_path}")
        except Exception as e:
            print(f"  [NFO] 生成失败: {e}")

    backup_databases()
    return True

# ---------- Driver ----------
def _create_chrome_instance():
    user_data_dir = tempfile.mkdtemp(prefix="missav_detail_")
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    if HEADLESS:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-logging')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-sync')
    options.add_argument('--disk-cache-size=0')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--window-size=800,600')
    options.add_argument('--disable-features=TranslateUI')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-background-timer-throttling')
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    kwargs = {'options': options, 'use_subprocess': False}
    if CHROMEDRIVER_PATH:
        kwargs['driver_executable_path'] = CHROMEDRIVER_PATH
    if CHROME_BIN:
        kwargs['browser_executable_path'] = CHROME_BIN

    with init_lock:
        driver = uc.Chrome(**kwargs)
    driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
    return driver

def get_detail_driver():
    if hasattr(thread_local, 'driver') and hasattr(thread_local, 'page_count'):
        if thread_local.page_count >= Config.DRIVER_MAX_PAGES:
            print(f"  [Driver] 已处理 {thread_local.page_count} 页，主动重启")
            reset_detail_driver()

    if not hasattr(thread_local, 'driver'):
        thread_local.driver = _create_chrome_instance()
        thread_local.page_count = 0
        driver_pool.append(thread_local.driver)

    driver = thread_local.driver
    try:
        driver.get('data:text/html,<html><head><title>Health</title></head><body>OK</body></html>')
    except Exception as e:
        print(f"  [Driver] 健康检查失败，强制重置: {e}")
        reset_detail_driver()
        return get_detail_driver()

    thread_local.page_count += 1
    return driver

def reset_detail_driver():
    if hasattr(thread_local, 'driver'):
        try:
            thread_local.driver.quit()
        except:
            pass
        delattr(thread_local, 'driver')
    if hasattr(thread_local, 'page_count'):
        delattr(thread_local, 'page_count')

def cleanup_drivers():
    for d in driver_pool:
        try:
            d.quit()
        except:
            pass
atexit.register(cleanup_drivers)

# ==================== 爬虫类 ====================
class MissAVDetailScraper:
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

    def safe_get(self, url, retries=5):
        for i in range(retries):
            try:
                self.driver.get(url)
                return True
            except Exception as e:
                print(f"  [GET] 第{i+1}次请求失败: {e}")
                if i == retries - 1:
                    print(f"  [GET] 连续超时，重置当前线程 driver")
                    reset_detail_driver()
                else:
                    time.sleep(5 * (i + 1))
        return False

    def get_video_details(self, video_url):
        details = {'url': video_url, 'code': '', 'title': '', 'm3u8': '', 'cover': ''}
        try:
            m = re.search(r'/cn/([^/?#]+)', video_url, re.I)
            if m: details['code'] = m.group(1).upper().replace('-UNCENSORED-LEAK', '')
            if not self.safe_get(video_url):
                return details
            if not self.wait_for_cloudflare():
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
                m = re.search(r'fourhoi\.com/([^/]+)/', details['cover'])
                if m:
                    code2 = m.group(1).upper()
                    if code2 != details['code']:
                        details['code'] = code2
            except:
                if details['code']:
                    details['cover'] = f"https://fourhoi.com/{details['code'].lower()}/cover-n.jpg"

            for attempt in range(1, Config.M3U8_RETRY_TIMES + 1):
                m3u8_candidate = ""
                try:
                    scripts = self.driver.find_elements(By.TAG_NAME, 'script')
                    for s in scripts:
                        content = s.get_attribute('outerHTML') or s.get_attribute('innerHTML') or ''
                        if 'm3u8' in content:
                            found = re.findall(r'(https?://[^\s\'\"<>]+\.m3u8[^\s\'\"<>]*)', content)
                            if found:
                                m3u8_candidate = found[0]
                                break
                except: pass
                if not m3u8_candidate:
                    try:
                        videos = self.driver.find_elements(By.TAG_NAME, 'video')
                        for v in videos:
                            src = v.get_attribute('src')
                            if src and '.m3u8' in src:
                                m3u8_candidate = src
                                break
                    except: pass
                if not m3u8_candidate:
                    try:
                        logs = self.driver.get_log('performance')
                        for entry in logs:
                            log = json.loads(entry['message'])['message']
                            if log.get('method') == 'Network.responseReceived':
                                url = log.get('params', {}).get('response', {}).get('url', '')
                                if '.m3u8' in url:
                                    m3u8_candidate = url
                                    break
                    except: pass
                if m3u8_candidate:
                    details['m3u8'] = m3u8_candidate.strip()
                    break
                else:
                    if attempt < Config.M3U8_RETRY_TIMES:
                        wait_time = min(attempt * Config.M3U8_RETRY_WAIT_BASE, 30)
                        print(f"  ⚠ 第{attempt}次未找到m3u8，等待{wait_time}秒刷新...")
                        time.sleep(wait_time)
                        self.driver.refresh()
                        if not self.wait_for_cloudflare():
                            if not self.safe_get(video_url):
                                break
                        else:
                            time.sleep(Config.PAGE_LOAD_WAIT_DETAIL)
                    else:
                        print(f"  ✗ 重试{Config.M3U8_RETRY_TIMES}次失败，放弃")
                        details['m3u8'] = ''
                        reset_detail_driver()
        except Exception as e:
            print(f"  [详情错误] {e}")
            reset_detail_driver()
        return details

def process_one_detail(video_url):
    time.sleep(random.uniform(1, 2))
    try:
        driver = get_detail_driver()
        scraper = MissAVDetailScraper(driver)
        details = scraper.get_video_details(video_url)
        if details['code'] and details['m3u8']:
            return save_video(details)
        else:
            return False
    except Exception as e:
        print(f"  [详情异常] {e}")
        reset_detail_driver()
        return False

def main():
    print("="*60)
    print("MissAV 详情抓取脚本 V1.0")
    print(f"无头模式: {HEADLESS}")
    print("="*60)

    init_databases()
    pending_entries = get_pending_log()
    if not pending_entries:
        print("没有待处理的任务，退出。")
        return

    total_success = 0
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=Config.MAX_DETAIL_WORKERS) as executor:
        while pending_entries:
            print(f"开始处理 {len(pending_entries)} 个待处理项...")
            futures = {}
            for code, url, _ in pending_entries:
                future = executor.submit(process_one_detail, url)
                futures[future] = (code, url)
            completed = 0
            for future in as_completed(futures):
                code, url = futures[future]
                completed += 1
                try:
                    success = future.result(timeout=Config.DETAIL_TASK_TIMEOUT)
                    if success:
                        total_success += 1
                except TimeoutError:
                    print(f"  {code} 超时，放弃并重置 driver")
                    reset_detail_driver()
                except Exception as e:
                    print(f"  {code} 异常: {e}")
                if completed % 10 == 0 or completed == len(pending_entries):
                    elapsed = time.time() - start_time
                    print(f"  进度: {completed}/{len(pending_entries)} | 成功: {total_success} | 运行 {elapsed:.0f}s")
                    if completed % 10 == 0:
                        backup_databases()
            pending_entries = get_pending_log()

    cleanup_drivers()
    print(f"\n任务完成！本次成功获取 {total_success} 部影片信息")
    backup_databases()

if __name__ == "__main__":
    main()
