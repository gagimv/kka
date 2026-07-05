#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 列表抓取脚本 V5.4 (版本匹配 + GitHub Actions 完全兼容)
- 自动获取 Chrome 主版本号，传递给 undetected-chromedriver 确保驱动匹配
- 保留所有断点续抓、实时入库、并发特性
- 修复 GitHub Actions 上 SessionNotCreatedException
"""

import ssl
import shutil
import subprocess

try:
    import certifi
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except:
    ssl._create_default_https_context = ssl._create_unverified_context

import os, sys, re, time, threading, tempfile, sqlite3
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore', category=ResourceWarning)

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
except ImportError:
    print("请先安装 undetected-chromedriver: pip install undetected-chromedriver")
    sys.exit(1)

HEADLESS = os.environ.get("HEADLESS", "false").lower() == "true"
LIST_MODE = os.environ.get("LIST_MODE", "incremental").lower()
RESCRAPE = os.environ.get("RESCRAPE", "false").lower() == "true"

class Config:
    MAX_LIST_WORKERS = 4
    DB_PATH = "videos.db"
    LOG_DB_PATH = "log.db"
    CLOUDFLARE_MAX_WAIT = 60
    PAGE_LOAD_TIMEOUT = 90
    PAGE_LOAD_WAIT_LIST = 5

save_lock = threading.Lock()
processed_codes_set = set()
_driver_cache_lock = threading.Lock()
_driver_path_cache = None
_chrome_bin_cache = None
_chrome_version_cache = None

# ---------- 数据库初始化 ----------
def init_databases():
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS pending (
                        code TEXT PRIMARY KEY,
                        url TEXT,
                        source_url TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS scrape_progress (
                        source_url TEXT PRIMARY KEY,
                        current_page INTEGER,
                        status TEXT
                    )''')
    conn.close()

    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
                        code TEXT PRIMARY KEY,
                        title TEXT,
                        m3u8 TEXT,
                        cover TEXT,
                        processed INTEGER DEFAULT 0,
                        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.close()

def load_processed_codes():
    global processed_codes_set
    if LIST_MODE != 'incremental':
        processed_codes_set = set()
        return
    conn = sqlite3.connect(Config.DB_PATH)
    cur = conn.execute("SELECT code FROM videos WHERE processed=1")
    processed_codes_set = {row[0] for row in cur.fetchall()}
    conn.close()

def update_progress(source_url, page, status):
    with save_lock:
        conn = sqlite3.connect(Config.LOG_DB_PATH)
        conn.execute('''INSERT OR REPLACE INTO scrape_progress (source_url, current_page, status)
                        VALUES (?, ?, ?)''', (source_url, page, status))
        conn.commit()
        conn.close()

def get_progress(source_url):
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    cur = conn.execute("SELECT current_page, status FROM scrape_progress WHERE source_url=?", (source_url,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return None, None

def insert_entries_to_pending(entries):
    if not entries:
        return
    with save_lock:
        conn = sqlite3.connect(Config.LOG_DB_PATH)
        conn.executemany('''INSERT OR IGNORE INTO pending (code, url, source_url)
                           VALUES (?, ?, ?)''', entries)
        conn.commit()
        conn.close()

def add_new_entries(entries):
    if not entries:
        return
    if LIST_MODE == 'full':
        insert_entries_to_pending(entries)
    else:
        new = [e for e in entries if e[0] not in processed_codes_set]
        if new:
            insert_entries_to_pending(new)
            print(f"  [入库] 新增 {len(new)} 个番号")

# ---------- 浏览器驱动管理 ----------
def _get_chrome_version(browser_path):
    """获取 Chrome 主版本号"""
    try:
        output = subprocess.check_output([browser_path, '--version'], stderr=subprocess.STDOUT).decode()
        match = re.search(r'(\d+)\.', output)
        if match:
            return int(match.group(1))
    except Exception as e:
        print(f"[警告] 无法获取浏览器版本: {e}")
    return None

def _find_chrome_binary():
    chrome_bin = os.environ.get("CHROME_BIN", "")
    if chrome_bin and not os.path.isfile(chrome_bin):
        print(f"[警告] CHROME_BIN 路径不存在: {chrome_bin}，自动查找")
        chrome_bin = ""
    if not chrome_bin:
        for name in ["google-chrome", "google-chrome-stable", "chrome", "chromium-browser"]:
            chrome_bin = shutil.which(name)
            if chrome_bin:
                print(f"[信息] 浏览器: {chrome_bin}")
                break
    if not chrome_bin:
        print("[错误] 未找到 Chrome 浏览器")
        sys.exit(1)
    return chrome_bin

def _download_and_patch_driver():
    global _driver_path_cache, _chrome_bin_cache, _chrome_version_cache
    with _driver_cache_lock:
        if _driver_path_cache:
            return _driver_path_cache, _chrome_bin_cache

        chrome_bin = _find_chrome_binary()
        version_main = _get_chrome_version(chrome_bin)
        if version_main:
            print(f"[信息] 浏览器主版本: {version_main}")
            _chrome_version_cache = version_main

        user_data_dir = tempfile.mkdtemp(prefix="missav_init_")
        options = uc.ChromeOptions()
        options.binary_location = chrome_bin
        if HEADLESS:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-logging')
        options.add_argument('--window-size=800,600')

        print("[信息] 下载/修补 chromedriver...")
        kwargs = {
            'options': options,
            'browser_executable_path': chrome_bin,
            'use_subprocess': False,
        }
        if version_main:
            kwargs['version_main'] = version_main      # 关键：强制匹配版本

        driver = uc.Chrome(**kwargs)
        driver_path = driver.service.path if hasattr(driver, 'service') and driver.service else None
        if not driver_path:
            try:
                driver_path = driver.patcher.executable_path
            except:
                pass
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)

        if not driver_path or not os.path.isfile(driver_path):
            print("[错误] 无法获取 chromedriver 路径")
            sys.exit(1)

        print(f"[信息] chromedriver 就绪: {driver_path}")
        _driver_path_cache = driver_path
        _chrome_bin_cache = chrome_bin
        return driver_path, chrome_bin

def _create_chrome_instance():
    driver_path, chrome_bin = _download_and_patch_driver()
    version_main = _chrome_version_cache

    user_data_dir = tempfile.mkdtemp(prefix="missav_worker_")
    options = uc.ChromeOptions()
    options.binary_location = chrome_bin
    if HEADLESS:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-logging')
    options.add_argument('--window-size=800,600')

    kwargs = {
        'options': options,
        'browser_executable_path': chrome_bin,
        'driver_executable_path': driver_path,
        'use_subprocess': False,
    }
    if version_main:
        kwargs['version_main'] = version_main

    driver = uc.Chrome(**kwargs)
    driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
    return driver, user_data_dir

# ---------- 页面抓取逻辑（保持不变） ----------
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
                print(f"  [GET] 第{i+1}次请求失败: {e}")
                if i == retries - 1:
                    print(f"  [GET] 连续超时")
                    return False
                time.sleep(5 * (i + 1))
        return False

    def fetch_list_pages(self, url, start_page=1, max_pages=None):
        print(f"  [列表] 开始 {url}（起始页 {start_page}）")
        all_count = 0
        base = url.rstrip('/')
        try:
            if start_page > 1:
                first_url = f"{base}{'&' if '?' in base else '?'}page={start_page}"
                if not self.safe_get(first_url):
                    return all_count
            else:
                if not self.safe_get(base):
                    return all_count
            if not self.wait_for_cloudflare():
                return all_count
            time.sleep(Config.PAGE_LOAD_WAIT_LIST)

            total = self.get_total_pages()
            if max_pages and total:
                total = min(total, start_page + max_pages - 1)
            elif max_pages:
                total = start_page + max_pages - 1

            current_page = start_page
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

                entries = [(v.split('/')[-1].upper().replace('-UNCENSORED-LEAK', ''), v, url) for v in videos]
                add_new_entries(entries)
                update_progress(url, current_page, 'in_progress')
                all_count += len(videos)

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

            update_progress(url, current_page if 'current_page' in locals() else start_page, 'completed')
            print(f"  [列表完成] 共 {all_count} 部")
            return all_count
        except Exception as e:
            print(f"  [列表错误] {e}")
            return all_count

def fetch_and_collect(url, start_page, max_pages):
    driver = None
    tmp_dir = None
    try:
        driver, tmp_dir = _create_chrome_instance()
        scraper = MissAVListScraper(driver)
        if any(k in url for k in ['/makers/', '/new', '/release', '/actresses/', '/genres/', '/tags/',
                                  '/cn/new', '/cn/release']):
            scraper.fetch_list_pages(url, start_page=start_page, max_pages=max_pages)
        else:
            code = url.split('/')[-1].upper().replace('-UNCENSORED-LEAK', '')
            add_new_entries([(code, url, 'direct')])
            update_progress(url, 1, 'completed')
    except Exception as e:
        print(f"[列表异常] {url}: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)

def main():
    print("="*60)
    print("MissAV 列表抓取脚本 V5.4 (版本匹配修复)")
    print(f"模式: {LIST_MODE} | 强制重抓: {RESCRAPE}")
    print("="*60)

    init_databases()
    load_processed_codes()

    url_file = "url.txt"
    if not os.path.exists(url_file):
        print(f"错误：未找到 {url_file}")
        sys.exit(1)

    with open(url_file, 'r', encoding='utf-8') as f:
        all_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not all_urls:
        print(f"错误：{url_file} 中没有有效 URL")
        sys.exit(1)

    start_page_env = int(os.environ.get('START_PAGE', '1'))
    end_page_env = os.environ.get('END_PAGE', '')
    max_pages_env = None
    if end_page_env:
        end_page = int(end_page_env)
        if end_page >= start_page_env:
            max_pages_env = end_page - start_page_env + 1

    tasks = []
    skip_count = 0
    for url in all_urls:
        page_progress, status = get_progress(url) if not RESCRAPE else (None, None)
        if status == 'completed' and not RESCRAPE:
            skip_count += 1
            continue

        if status == 'in_progress':
            start_page = page_progress + 1
        else:
            start_page = start_page_env

        max_pages = None
        if max_pages_env:
            if status == 'in_progress':
                remaining = max_pages_env - page_progress
                if remaining <= 0:
                    continue
                max_pages = remaining
            else:
                max_pages = max_pages_env

        tasks.append((url, start_page, max_pages))

    print(f"从 {url_file} 读取到 {len(all_urls)} 个链接，跳过已完成 {skip_count}，待处理 {len(tasks)}")

    if not tasks:
        print("所有链接均已完成，无需抓取。")
        return

    print("[主线程] 准备 chromedriver...")
    _download_and_patch_driver()
    print("[主线程] 驱动就绪，开始并发抓取\n")

    with ThreadPoolExecutor(max_workers=Config.MAX_LIST_WORKERS) as executor:
        futures = {executor.submit(fetch_and_collect, url, sp, mp): url for url, sp, mp in tasks}
        for f in as_completed(futures):
            url = futures[f]
            try:
                f.result()
            except Exception as e:
                print(f"任务异常 [{url}]: {e}")

    print("\n========== 进度摘要 ==========")
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    cur = conn.execute("SELECT source_url, current_page, status FROM scrape_progress")
    for row in cur.fetchall():
        print(f"  [{row[2]}] {row[0]} → 最后页: {row[1]}")
    conn.close()

    conn = sqlite3.connect(Config.LOG_DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
    conn.close()
    print(f"log.db 待处理记录数: {count}")

if __name__ == "__main__":
    main()
