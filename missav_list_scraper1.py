#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 列表抓取脚本 V4.0 (单次下载驱动，避免并发冲突)
从 url.txt 读取目标链接，抓取影片信息写入待处理队列 (log.db)
支持全量/增量模式，自动匹配浏览器版本下载驱动，适配 GitHub Actions 并发
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

class Config:
    MAX_LIST_WORKERS = 4
    DB_PATH = "videos.db"
    LOG_DB_PATH = "log.db"
    COMP_DB_PATH = "comp.db"
    CLOUDFLARE_MAX_WAIT = 60
    PAGE_LOAD_TIMEOUT = 90
    PAGE_LOAD_WAIT_LIST = 5
    QUEUE_BATCH_SIZE = 50

save_lock = threading.Lock()
# 驱动全局缓存与下载锁
_driver_cache_lock = threading.Lock()
_driver_path_cache = None
_chrome_bin_cache = None

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
    """查找 Chrome 浏览器路径（仅执行一次）"""
    chrome_bin = os.environ.get("CHROME_BIN", "")
    if chrome_bin and not os.path.isfile(chrome_bin):
        print(f"[警告] 环境变量 CHROME_BIN 指定的路径不存在: {chrome_bin}，将自动查找")
        chrome_bin = ""

    if not chrome_bin:
        for name in ["google-chrome", "google-chrome-stable", "chrome", "chromium-browser"]:
            chrome_bin = shutil.which(name)
            if chrome_bin:
                print(f"[信息] 从 PATH 找到浏览器: {chrome_bin}")
                break

    if not chrome_bin:
        print("[错误] 未找到可用的 Chrome 浏览器")
        sys.exit(1)
    return chrome_bin

def _download_and_patch_driver():
    """
    线程安全地下载并修补 chromedriver，返回驱动可执行文件的绝对路径。
    只有第一次调用会真正执行下载，后续直接返回缓存路径。
    """
    global _driver_path_cache, _chrome_bin_cache
    with _driver_cache_lock:
        if _driver_path_cache:
            return _driver_path_cache, _chrome_bin_cache

        # 查找浏览器
        chrome_bin = _find_chrome_binary()
        version_main = _get_chrome_version(chrome_bin)
        if version_main:
            print(f"[信息] 浏览器主版本: {version_main}，准备下载对应 chromedriver")

        # 创建一个临时驱动实例，让 undetected_chromedriver 自动下载
        user_data_dir = tempfile.mkdtemp(prefix="missav_init_")
        options = uc.ChromeOptions()
        options.add_argument(f"--user-data-dir={user_data_dir}")
        if HEADLESS:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-logging')

        kwargs = {
            'options': options,
            'use_subprocess': False,
            'browser_executable_path': chrome_bin,
        }
        if version_main:
            kwargs['version_main'] = version_main

        print("[信息] 开始下载/修补 chromedriver...")
        driver = uc.Chrome(**kwargs)
        # 获取实际使用的驱动路径
        driver_path = driver.service.path if hasattr(driver, 'service') and driver.service else None
        if not driver_path:
            # 备用方法：从 driver 的 capabilities 或 patcher 获取
            try:
                driver_path = driver.patcher.executable_path
            except:
                pass
        driver.quit()
        import shutil as _shutil
        _shutil.rmtree(user_data_dir, ignore_errors=True)

        if not driver_path or not os.path.isfile(driver_path):
            print("[错误] 无法获取 chromedriver 路径")
            sys.exit(1)

        print(f"[信息] chromedriver 已就绪: {driver_path}")
        _driver_path_cache = driver_path
        _chrome_bin_cache = chrome_bin
        return driver_path, chrome_bin

def _create_chrome_instance():
    """
    创建 Chrome 实例，复用已下载的驱动路径，避免并发下载冲突。
    """
    driver_path, chrome_bin = _download_and_patch_driver()

    user_data_dir = tempfile.mkdtemp(prefix="missav_worker_")
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    if HEADLESS:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-logging')
    options.add_argument('--window-size=800,600')

    kwargs = {
        'options': options,
        'use_subprocess': False,
        'browser_executable_path': chrome_bin,
        'driver_executable_path': driver_path,  # 关键：直接指定已下载的驱动
    }

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
            collector([(url.split('/')[-1].upper().replace('-UNCENSORED-LEAK', ''), url, 'direct')])
    except Exception as e:
        print(f"[列表异常] {url}: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass

def main():
    print("="*60)
    print("MissAV 列表抓取脚本 V4.0 (防并发驱动下载)")
    print(f"模式: {LIST_MODE}")
    print("="*60)

    init_databases()

    url_file = "url.txt"
    if not os.path.exists(url_file):
        print(f"错误：未找到 {url_file}")
        sys.exit(1)

    with open(url_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not urls:
        print(f"错误：{url_file} 中没有有效的 URL")
        sys.exit(1)

    start_page_str = os.environ.get('START_PAGE', '').strip()
    end_page_str = os.environ.get('END_PAGE', '').strip()
    start_page = int(start_page_str) if start_page_str else 1
    max_pages = None
    if end_page_str:
        end_page = int(end_page_str)
        max_pages = end_page - start_page + 1

    print(f"从 {url_file} 读取到 {len(urls)} 个目标 URL")
    print(f"页码范围：第 {start_page} 页 到 {'最后' if not end_page_str else f'第 {end_page} 页'}")

    # ---------- 主线程预先下载驱动 ----------
    print("[主线程] 正在准备 chromedriver（仅一次）...")
    _download_and_patch_driver()  # 确保下载完成
    print("[主线程] 驱动准备完毕，开始并发抓取\n")

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
    else:
        update_comp_db(all_entries)
        new_entries = get_codes_to_process()
        if new_entries:
            add_to_log_batch(new_entries)
            print(f"增量模式：发现 {len(new_entries)} 个新番号，已写入待处理队列")
        else:
            print("增量模式：没有新番号需要添加")

if __name__ == "__main__":
    main()
