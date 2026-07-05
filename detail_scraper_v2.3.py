#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MissAV 详情抓取脚本 V2.4 (硬编码 Chrome 路径 + 版本匹配)
- 从 urls.txt 读取详情页 URL，提取 m3u8、封面及完整元数据
- 自动匹配 Chrome 驱动版本，适合 GitHub Actions
- 只存入 videos.db，不生成 .nfo 文件
- 支持通过 CHROME_BIN 环境变量或硬编码路径查找 Chrome
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
    MAX_DETAIL_WORKERS = 12
    DB_PATH = "videos.db"
    BACKUP_DIR = "/tmp/missav_backup"
    CLOUDFLARE_MAX_WAIT = 60
    PAGE_LOAD_TIMEOUT = 90
    PAGE_LOAD_WAIT_DETAIL = 8
    M3U8_RETRY_TIMES = 10
    M3U8_RETRY_WAIT_BASE = 2
    DRIVER_MAX_PAGES = 20
    DETAIL_TASK_TIMEOUT = 120

# ---------- 浏览器驱动全局缓存（版本匹配核心） ----------
init_lock = threading.Lock()
save_lock = threading.Lock()
thread_local = threading.local()
driver_pool = []

_driver_cache_lock = threading.Lock()
_driver_path_cache = None
_chrome_bin_cache = None
_chrome_version_cache = None

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
    """
    查找 Chrome 二进制文件，优先级：
    1. 环境变量 CHROME_BIN
    2. 硬编码的常用路径（适配 GitHub Actions setup-chrome）
    3. 系统 PATH 搜索
    """
    # 1. 环境变量
    chrome_bin = os.environ.get("CHROME_BIN", "")
    if chrome_bin and os.path.isfile(chrome_bin) and os.access(chrome_bin, os.X_OK):
        print(f"[信息] 浏览器 (env): {chrome_bin}")
        return chrome_bin

    # 2. 硬编码路径（常见安装位置）
    hardcoded_paths = [
        "/opt/google/chrome/chrome",            # setup-chrome@v1 默认路径
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/usr/local/bin/google-chrome",
        "/snap/bin/chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",  # macOS
    ]
    for path in hardcoded_paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            print(f"[信息] 浏览器 (硬编码): {path}")
            return path

    # 3. 系统 PATH 搜索
    for name in ["google-chrome", "google-chrome-stable", "chrome", "chromium-browser", "chromium"]:
        chrome_bin = shutil.which(name)
        if chrome_bin:
            print(f"[信息] 浏览器 (which): {chrome_bin}")
            return chrome_bin

    print("[错误] 未找到 Chrome 浏览器，请安装或设置 CHROME_BIN 环境变量。")
    sys.exit(1)

def _download_and_patch_driver():
    """下载/修补 chromedriver，使版本与浏览器匹配，并缓存路径"""
    global _driver_path_cache, _chrome_bin_cache, _chrome_version_cache
    with _driver_cache_lock:
        if _driver_path_cache:
            return _driver_path_cache, _chrome_bin_cache

        chrome_bin = _find_chrome_binary()
        version_main = _get_chrome_version(chrome_bin)
        if version_main:
            print(f"[信息] 浏览器主版本: {version_main}")
            _chrome_version_cache = version_main

        user_data_dir = tempfile.mkdtemp(prefix="missav_detail_init_")
        options = uc.ChromeOptions()
        options.binary_location = chrome_bin
        if HEADLESS:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-logging')
        options.add_argument('--window-size=800,600')

        print("[信息] 下载/修补 chromedriver（用于详情抓取）...")
        kwargs = {
            'options': options,
            'browser_executable_path': chrome_bin,
            'use_subprocess': False,
        }
        if version_main:
            kwargs['version_main'] = version_main

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

# ---------- 备份 ----------
def backup_databases():
    os.makedirs(Config.BACKUP_DIR, exist_ok=True)
    db_path = Config.DB_PATH
    if os.path.exists(db_path):
        try:
            shutil.copy2(db_path, os.path.join(Config.BACKUP_DIR, os.path.basename(db_path)))
        except Exception as e:
            print(f"  ⚠ 备份 {db_path} 失败: {e}")

# ---------- 数据库 ----------
def init_database():
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
    migrate_database()

def migrate_database():
    """为旧 videos 表添加新列（如果不存在）"""
    new_columns = {
        'description': 'TEXT',
        'release_date': 'TEXT',
        'raw_code': 'TEXT',
        'actress': 'TEXT',
        'genres': 'TEXT',
        'series': 'TEXT',
        'studio': 'TEXT',
        'director': 'TEXT',
        'label': 'TEXT'
    }
    conn = sqlite3.connect(Config.DB_PATH)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(videos)")}
    for col, col_type in new_columns.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE videos ADD COLUMN {col} {col_type}")
                print(f"[数据库] 添加列: {col}")
            except Exception as e:
                print(f"[数据库] 添加列 {col} 失败: {e}")
    conn.commit()
    conn.close()

def is_already_processed(code):
    """检查 code 是否已有 m3u8 且非空，用于增量模式跳过"""
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
    backup_databases()
    return True

# ---------- Driver ----------
def _create_chrome_instance():
    driver_path, chrome_bin = _download_and_patch_driver()
    version_main = _chrome_version_cache

    user_data_dir = tempfile.mkdtemp(prefix="missav_detail_")
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    options.binary_location = chrome_bin
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

    kwargs = {
        'options': options,
        'browser_executable_path': chrome_bin,
        'driver_executable_path': driver_path,
        'use_subprocess': False,
    }
    if version_main:
        kwargs['version_main'] = version_main

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

    def _extract_detail_fields(self):
        """提取完整元数据字段"""
        fields = {
            'description': '', 'release_date': '', 'raw_code': '',
            'title': '', 'actress': '', 'genres': '', 'series': '',
            'studio': '', 'director': '', 'label': ''
        }
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@x-show=\"currentTab === 'video_details'\"]"))
            )
            detail_div = self.driver.find_element(By.XPATH, "//div[@x-show=\"currentTab === 'video_details'\"]")

            # 简介
            try:
                desc = detail_div.find_element(By.XPATH, ".//div[contains(@class,'line-clamp')]")
                fields['description'] = desc.text.strip()
            except: pass

            # 发行日期
            try:
                date_elem = detail_div.find_element(By.XPATH, ".//time")
                fields['release_date'] = date_elem.get_attribute('datetime') or date_elem.text.strip()
            except: pass

            # 番号
            try:
                span = detail_div.find_element(By.XPATH, ".//span[text()='番号:']/following-sibling::span")
                fields['raw_code'] = span.text.strip()
            except: pass

            # 标题
            try:
                span = detail_div.find_element(By.XPATH, ".//span[text()='标题:']/following-sibling::span")
                fields['title'] = span.text.strip()
            except: pass

            # 女优
            try:
                elems = detail_div.find_elements(By.XPATH, ".//span[text()='女优:']/following-sibling::a")
                fields['actress'] = ', '.join([a.text.strip() for a in elems if a.text.strip()])
            except: pass

            # 类型
            try:
                elems = detail_div.find_elements(By.XPATH, ".//span[text()='类型:']/following-sibling::a")
                fields['genres'] = ', '.join([a.text.strip() for a in elems if a.text.strip()])
            except: pass

            # 系列
            try:
                elems = detail_div.find_elements(By.XPATH, ".//span[text()='系列:']/following-sibling::a")
                fields['series'] = ', '.join([a.text.strip() for a in elems if a.text.strip()])
            except: pass

            # 发行商
            try:
                elems = detail_div.find_elements(By.XPATH, ".//span[text()='发行商:']/following-sibling::a")
                fields['studio'] = ', '.join([a.text.strip() for a in elems if a.text.strip()])
            except: pass

            # 导演
            try:
                elems = detail_div.find_elements(By.XPATH, ".//span[text()='导演:']/following-sibling::a")
                fields['director'] = ', '.join([a.text.strip() for a in elems if a.text.strip()])
            except: pass

            # 标签
            try:
                elems = detail_div.find_elements(By.XPATH, ".//span[text()='标籤:']/following-sibling::a")
                fields['label'] = ', '.join([a.text.strip() for a in elems if a.text.strip()])
            except: pass
        except Exception as e:
            print(f"  [元数据提取] 异常: {e}")
        return fields

    def get_video_details(self, video_url):
        details = {
            'url': video_url, 'code': '', 'cover': '', 'm3u8': '',
            'description': '', 'release_date': '', 'raw_code': '', 'title': '',
            'actress': '', 'genres': '', 'series': '', 'studio': '', 'director': '', 'label': ''
        }
        try:
            # 从URL提取标准code（去除后缀）
            m = re.search(r'/cn/([^/?#]+)', video_url, re.I)
            if m:
                raw = m.group(1).upper()
                details['code'] = raw.replace('-UNCENSORED-LEAK', '')
            else:
                return details

            if not self.safe_get(video_url):
                return details
            if not self.wait_for_cloudflare():
                return details
            time.sleep(Config.PAGE_LOAD_WAIT_DETAIL)

            # 尝试从h1获取标题作为后备
            try:
                h1 = self.driver.find_element(By.TAG_NAME, 'h1')
                if h1.text.strip() != 'missav.ws' and not details.get('title'):
                    details['title'] = h1.text.strip()
            except: pass

            # 封面
            try:
                og = self.driver.find_element(By.CSS_SELECTOR, 'meta[property="og:image"]')
                details['cover'] = og.get_attribute('content')
            except:
                if details['code']:
                    details['cover'] = f"https://fourhoi.com/{details['code'].lower()}/cover-n.jpg"

            # 提取详细元数据
            meta = self._extract_detail_fields()
            details.update(meta)

            if not details.get('title'):
                details['title'] = meta.get('title', '')

            # 查找 m3u8
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

def process_one_detail(url):
    time.sleep(random.uniform(1, 2))
    m = re.search(r'/cn/([^/?#]+)', url, re.I)
    if not m:
        print(f"  ⚠ 无法从 URL 提取 code: {url}")
        return False
    raw_code = m.group(1).upper().replace('-UNCENSORED-LEAK', '')
    if is_already_processed(raw_code):
        print(f"  ⊙ {raw_code} 已有 m3u8，跳过（可设置 RESCRAPE=true 强制重抓）")
        return False

    try:
        driver = get_detail_driver()
        scraper = MissAVDetailScraper(driver)
        details = scraper.get_video_details(url)
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
    print("MissAV 详情抓取脚本 V2.4 (硬编码 Chrome 路径)")
    print(f"无头模式: {HEADLESS} | 强制重抓: {RESCRAPE}")
    print("="*60)

    init_database()
    url_file = "urls.txt"
    if not os.path.exists(url_file):
        print(f"错误：未找到 {url_file}")
        sys.exit(1)

    with open(url_file, 'r', encoding='utf-8') as f:
        all_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    if not all_urls:
        print(f"错误：{url_file} 中没有有效 URL")
        sys.exit(1)

    print(f"从 {url_file} 读取到 {len(all_urls)} 个链接")

    print("[主线程] 准备 chromedriver...")
    _download_and_patch_driver()
    print("[主线程] 驱动就绪，开始并发抓取详情\n")

    total_success = 0
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=Config.MAX_DETAIL_WORKERS) as executor:
        futures = {executor.submit(process_one_detail, url): url for url in all_urls}
        completed = 0
        for future in as_completed(futures):
            url = futures[future]
            completed += 1
            try:
                success = future.result(timeout=Config.DETAIL_TASK_TIMEOUT)
                if success:
                    total_success += 1
            except TimeoutError:
                print(f"  ⚠ 任务超时: {url}")
            except Exception as e:
                print(f"  [任务异常] {url}: {e}")
            if completed % 10 == 0 or completed == len(all_urls):
                elapsed = time.time() - start_time
                print(f"  进度: {completed}/{len(all_urls)} | 成功: {total_success} | 运行 {elapsed:.0f}s")

    cleanup_drivers()
    print(f"\n任务完成！本次成功获取 {total_success} 部影片信息")
    backup_databases()

if __name__ == "__main__":
    main()
