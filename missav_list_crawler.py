#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 列表抓取脚本 V1.0
从分页列表中获取影片链接，写入待处理队列 (log.db)
支持全量模式和增量模式（只添加新番号）
"""

import ssl
try:
    import certifi
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except:
    ssl._create_default_https_context = ssl._create_unverified_context

import os, sys, re, time, threading, tempfile, sqlite3, atexit
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
LIST_MODE = os.environ.get("LIST_MODE", "incremental").lower()  # incremental 或 full

class Config:
    MAX_LIST_WORKERS = 4
    DB_PATH = "videos.db"
    LOG_DB_PATH = "log.db"
    COMP_DB_PATH = "comp.db"
    CLOUDFLARE_MAX_WAIT = 60
    PAGE_LOAD_TIMEOUT = 90
    PAGE_LOAD_WAIT_LIST = 5
    QUEUE_BATCH_SIZE = 50

CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH")
CHROME_BIN = os.environ.get("CHROME_BIN")
save_lock = threading.Lock()
driver_pool = []

def backup_databases():
    pass  # 列表脚本无需备份

def init_databases():
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS pending (
                        code TEXT PRIMARY KEY,
                        url TEXT,
                        source_url TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.close()

    conn = sqlite3.connect(Config.COMP_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS recent_list (
                        code TEXT PRIMARY KEY,
                        url TEXT,
                        source_url TEXT,
                        found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.close()

def add_to_log_batch(entries):
    if not entries:
        return
    conn = sqlite3.connect(Config.LOG_DB_PATH)
    with save_lock:
        conn.executemany('''INSERT OR IGNORE INTO pending (code, url, source_url)
                           VALUES (?, ?, ?)''', entries)
        conn.commit()
    conn.close()

def update_comp_db(entries):
    conn = sqlite3.connect(Config.COMP_DB_PATH)
    with save_lock:
        conn.execute("DELETE FROM recent_list")
        conn.executemany("INSERT OR IGNORE INTO recent_list (code, url, source_url) VALUES (?, ?, ?)", entries)
        conn.commit()
    conn.close()

def load_processed_codes():
    conn = sqlite3.connect(Config.DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS videos (
                        code TEXT PRIMARY KEY,
                        title TEXT,
                        m3u8 TEXT,
                        cover TEXT,
                        processed INTEGER DEFAULT 0,
                        updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    cur = conn.execute("SELECT code FROM videos WHERE processed=1")
    codes = {row[0] for row in cur.fetchall()}
    conn.close()
    return codes

def get_codes_to_process():
    conn_comp = sqlite3.connect(Config.COMP_DB_PATH)
    conn_videos = sqlite3.connect(Config.DB_PATH)
    conn_log = sqlite3.connect(Config.LOG_DB_PATH)

    comp_codes = {row[0]: (row[0], row[1], row[2]) for row in conn_comp.execute("SELECT code, url, source_url FROM recent_list")}
    processed_codes = {row[0] for row in conn_videos.execute("SELECT code FROM videos WHERE processed=1")}
    log_codes = {row[0] for row in conn_log.execute("SELECT code FROM pending")}

    new_entries = []
    for code, (c, url, src) in comp_codes.items():
        if code not in processed_codes and code not in log_codes:
            new_entries.append((c, url, src))

    conn_comp.close()
    conn_videos.close()
    conn_log.close()
    return new_entries

def _create_chrome_instance():
    user_data_dir = tempfile.mkdtemp(prefix="missav_list_")
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    if HEADLESS:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-logging')
    options.add_argument('--window-size=800,600')

    kwargs = {'options': options, 'use_subprocess': False}
    if CHROMEDRIVER_PATH:
        kwargs['driver_executable_path'] = CHROMEDRIVER_PATH
    if CHROME_BIN:
        kwargs['browser_executable_path'] = CHROME_BIN

    driver = uc.Chrome(**kwargs)
    driver.set_page_load_timeout(Config.PAGE_LOAD_TIMEOUT)
    return driver

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
                    print(f"  [GET] 连续超时，重置 driver")
                    return False
                time.sleep(5 * (i + 1))
        return False

    def fetch_list_pages(self, url, start_page=1, max_pages=None, log_callback=None):
        print(f"  [列表] 开始处理 {url}（起始页 {start_page}）")
        all_videos = []
        base = url.rstrip('/')
        try:
            if start_page > 1:
                first_url = f"{base}{'&' if '?' in base else '?'}page={start_page}"
                if not self.safe_get(first_url):
                    return all_videos
            else:
                if not self.safe_get(base):
                    return all_videos
            if not self.wait_for_cloudflare():
                return all_videos
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

                if log_callback and videos:
                    entries = [(v.split('/')[-1].upper().replace('-UNCENSORED-LEAK', ''), v, url) for v in videos]
                    log_callback(entries)
                all_videos.extend(videos)

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
            print(f"  [列表完成] 共 {len(all_videos)} 部")
            return all_videos
        except Exception as e:
            print(f"  [列表错误] {e}")
            return all_videos

def fetch_and_collect(url, start_page, max_pages, collector):
    driver = None
    try:
        driver = _create_chrome_instance()
        scraper = MissAVListScraper(driver)
        if any(k in url for k in ['/makers/', '/new', '/release', '/actresses/', '/genres/', '/tags/',
                                  '/cn/new', '/cn/release']):
            scraper.fetch_list_pages(url, start_page=start_page, max_pages=max_pages, log_callback=collector)
        else:
            # 单个影片链接直接加入
            collector([(url.split('/')[-1].upper().replace('-UNCENSORED-LEAK', ''), url, 'direct')])
    except Exception as e:
        print(f"[列表异常] {url}: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass

def main():
    print("="*60)
    print("MissAV 列表抓取脚本 V1.0")
    print(f"模式: {LIST_MODE}")
    print("="*60)

    init_databases()
    urls_str = os.environ.get('URLS', '').strip()
    if not urls_str:
        print("错误：未提供 URL，请通过环境变量 URLS 设置（逗号分隔）")
        sys.exit(1)
    urls = [u.strip() for u in urls_str.split(',') if u.strip()]

    start_page_str = os.environ.get('START_PAGE', '').strip()
    end_page_str = os.environ.get('END_PAGE', '').strip()
    start_page = int(start_page_str) if start_page_str else 1
    max_pages = None
    if end_page_str:
        end_page = int(end_page_str)
        max_pages = end_page - start_page + 1

    print(f"目标 URL：{urls}")
    print(f"页码范围：第 {start_page} 页 到 {'最后' if not end_page_str else f'第 {end_page} 页'}")

    # 收集所有链接
    all_entries = []
    entry_lock = threading.Lock()

    def collector(entries):
        with entry_lock:
            all_entries.extend(entries)

    with ThreadPoolExecutor(max_workers=Config.MAX_LIST_WORKERS) as executor:
        futures = [executor.submit(fetch_and_collect, url, start_page, max_pages, collector) for url in urls]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"抓取任务异常: {e}")

    print(f"本次共抓取到 {len(all_entries)} 条影片链接")

    if LIST_MODE == 'full':
        add_to_log_batch(all_entries)
        print(f"已将所有链接写入 log.db (pending)，共 {len(all_entries)} 条")
    else:  # incremental
        update_comp_db(all_entries)
        new_entries = get_codes_to_process()
        if new_entries:
            add_to_log_batch(new_entries)
            print(f"增量模式：发现 {len(new_entries)} 个新番号，已写入待处理队列")
        else:
            print("增量模式：没有新番号需要添加")

if __name__ == "__main__":
    main()
