import concurrent.futures
import time
import random
import threading
import re
from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests

# ── Налаштування ──────────────────────────────────────────────
BASE_URL = "https://growex.market"

CATEGORIES = {
    "ЗЗР (загальний)":      "/products/zasobi-zahistu-roslin-zzr",
    "Гербіциди":            "/products/gerbicidi",
    "Інсектициди":          "/products/insekticidi",
    "Акарициди":            "/products/akaricidi",
    "Родентициди":          "/products/rodenticidi",
    "Протруйники насіння":  "/products/protruyniki-nasinnya",
    "Фунгіциди":            "/products/fungicidi",
    "Інокулянти":           "/products/inokulyanti",
    "Регулятори росту":     "/products/regulyatori-rostu",
    "Допоміжні засоби":     "/products/dopomizhni-zasobi",
    "Фуміганти":            "/products/fumiganti",
    "Біологічні препарати": "/products/biologichni-preparati",
    "Пакети для фермера":   "/products/paketi-dlya-fermera",
    "Антисептики":          "/products/antiseptiki",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8",
    "Referer": "https://growex.market/",
}

MAX_WORKERS           = 8  
RETRY_DELAYS          = [3, 6, 12] # Трохи збільшили паузи для стабільності проксі

# Global proxy list
WORKING_PROXIES = []
_proxy_lock = threading.Lock()

# ── Статистика ────────────────────────────────────────────────
_stats_lock = threading.Lock()
STATS = {"403": 0, "429": 0, "errors": 0}

def _record(kind: str):
    with _stats_lock:
        STATS[kind] = STATS.get(kind, 0) + 1

# ── Автоматичний збір свіжих безкоштовних проксі ──────────────
def fetch_free_proxies():
    """Збирає список свіжих HTTPS проксі з безкоштовного API"""
    global WORKING_PROXIES
    print("⏳ Отримання свіжих безкоштовних проксі...")
    links = [
        "https://pubproxy.com/api/proxy?limit=5&format=txt&http=true&country=UA,PL,DE,FR,RO,BG,MD&level=anonymous",
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
    ]
    
    collected = []
    # Спроба витягнути з резервного простого списку проксі
    try:
        r = requests.get("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", timeout=10)
        if r.status_code == 200:
            lines = r.text.splitlines()
            collected = [line.strip() for line in lines if line.strip() and ":" in line]
            random.shuffle(collected)
            collected = collected[:30] # Беремо перші 30 випадкових для ротації
    except Exception as e:
        print(f"⚠️ Не вдалося зібрати проксі з github: {e}")
        
    with _proxy_lock:
        WORKING_PROXIES = collected
    print(f"✅ Знайдено {len(WORKING_PROXIES)} потенційних безкоштовних проксі для обходу блокування GitHub.")

def get_random_proxy() -> Optional[Dict[str, str]]:
    with _proxy_lock:
        if not WORKING_PROXIES:
            return None
        px = random.choice(WORKING_PROXIES)
        return {"http": f"http://{px}", "https": f"http://{px}"}

# ── Потокобезпечна сесія з підтримкою HTTP/2 та Проксі ────────
thread_local = threading.local()

def get_session(renew_proxy: bool = False) -> requests.Session:
    if not hasattr(thread_local, "session") or renew_proxy:
        s = requests.Session(impersonate="chrome120")
        s.headers.update(HEADERS)
        
        proxy = get_random_proxy()
        if proxy:
            s.proxies = proxy
        thread_local.session = s
    return thread_local.session

# ── Швидкий GET з ротацією проксі при помилках ────────────────
def safe_get(url: str, retries: int = len(RETRY_DELAYS)) -> Optional[str]:
    # Для кожного ретраю пробуємо свіже або інше проксі, якщо зловили 403
    for attempt in range(retries):
        session = get_session(renew_proxy=(attempt > 0))
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 200:
                if "cloudflare" in resp.text.lower() or "captcha" in resp.text.lower():
                    # Якщо Cloudflare пропустив з кодом 200, але там заглушка-капча
                    _record("403")
                    continue
                return resp.text
            if resp.status_code == 404:
                return None
            if resp.status_code in [403, 429]:
                _record(str(resp.status_code))
                time.sleep(RETRY_DELAYS[attempt] + random.uniform(1, 2))
                continue
        except Exception:
            _record("errors")
            time.sleep(RETRY_DELAYS[attempt])
    return None

# ── Розумне визначення кількості сторінок у категорії ─────────
def get_max_pages(category_path: str) -> int:
    url = f"{BASE_URL}{category_path}?page=1"
    html = safe_get(url)
    if not html:
        print(f"    ⚠️ Не вдалося прочитати {category_path} навіть через проксі (можливо, невдалий IP, беремо 1 сторінку)")
        return 1
    
    soup = BeautifulSoup(html, "lxml")
    pagination_links = soup.select("ul.pagination a[href*='page=']") or soup.select("a[href*='page=']")
    if not pagination_links:
        if soup.select(".card_product"):
            return 1
        return 1
    
    max_page = 1
    for link in pagination_links:
        href = link.get("href", "")
        match = re.search(r'page=(\d+)', href)
        if match:
            page_num = int(match.group(1))
            if page_num > max_page:
                max_page = page_num
                
    return max_page

# ── Парсинг карток ────────────────────────────────────────────
def parse_cards(soup: BeautifulSoup, category_name: str, page: int) -> List[Dict]:
    cards = soup.select(".card_product")
    rows = []
    for card in cards:
        a_tag = card.select_one("a[href*='/product/']")
        if not a_tag:
            continue
        href = a_tag.get("href", "")
        price_tag = card.select_one("div.card_product-price")
        rows.append({
            "Назва":     a_tag.get_text(strip=True),
            "Ціна":      price_tag.get_text(strip=True) if price_tag else "",
            "URL":       href if href.startswith("http") else BASE_URL + href,
            "Категорія": category_name,
            "Сторінка":  page,
        })
    return rows

# ── Парсинг однієї сторінки ───────────────────────────────────
def load_page(category_name: str, url_base: str, page: int) -> List[Dict]:
    url = f"{BASE_URL}{url_base}?page={page}"
    html = safe_get(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    return parse_cards(soup, category_name, page)

# ── КРОК 1: Динамічний збір каталогу ──────────────────────────
def collect_catalog() -> pd.DataFrame:
    print(f"\n=== Крок 1: Повний збір каталогу через ПРОКСІ-РОТАЦІЮ ({MAX_WORKERS} пар. потоків) ===")
    all_rows = []
    
    # Збираємо пули адрес перед основним парсингом
    fetch_free_proxies()
    
    print("Аналіз кількості сторінок в категоріях...")
    category_pages = {}
    for name, path in CATEGORIES.items():
        max_p = get_max_pages(path)
        category_pages[name] = max_p
        print(f"  -> {name}: визначено сторінок — {max_p}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for name, path in CATEGORIES.items():
            max_p = category_pages[name]
            for page in range(1, max_p + 1):  
                futures.append(executor.submit(load_page, name, path, page))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res:
                    all_rows.extend(res)
            except Exception as e:
                print(f"  ✗ Помилка потоку: {e}")
                
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
        
    before = len(df)
    df['is_general'] = df['Категорія'] == "ЗЗР (загальний)"
    df = df.sort_values(by='is_general').drop(columns=['is_general'])
    df = df.drop_duplicates(subset=["URL"]).reset_index(drop=True)
    print(f"Всього знайдено посилань: {before} | Унікальних товарів: {len(df)}")
    return df

# ── КРОК 2: Деталі товарів ────────────────────────────────────
def get_details(item: Dict) -> Dict:
    html = safe_get(item["URL"])
    if not html:
        item["Виробник"], item["Ціна_за_літр"] = "", ""
        return item
        
    soup = BeautifulSoup(html, "lxml")
    brand_tag   = soup.select_one("a[href*='/brand/']")
    price_l_tag = soup.select_one("div.one_price")
    
    item["Виробник"]     = brand_tag.get_text(strip=True) if brand_tag else ""
    item["Ціна_за_літр"] = price_l_tag.get_text(strip=True) if price_l_tag else ""
    return item

def enrich_with_details(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n=== Крок 2: Збір деталей через ПРОКСІ ({len(df)} шт., {MAX_WORKERS} потоків) ===")
    records = df.to_dict("records")
    enriched = []
    done = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(get_details, item) for item in records]
        for future in concurrent.futures.as_completed(futures):
            done += 1
            if done % 100 == 0 or done == len(records):
                print(f"  Прогрес: {done}/{len(records)}")
            enriched.append(future.result())
            
    return pd.DataFrame(enriched)

# ── Збереження ────────────────────────────────────────────────
def clean_and_save(df: pd.DataFrame, parse_date: str) -> str:
    df["Ціна"] = df["Ціна"].astype(str).str.replace("грн.", "", regex=False).str.strip()
    df["Дата_парсінгу"] = parse_date
    cols = ["Назва", "Ціна", "Ціна_за_літр", "Виробник", "Категорія", "URL", "Сторінка", "Дата_парсінгу"]
    df = df[[c for c in cols if c in df.columns]]
    
    out_file = "growex_zzr.xlsx"
    df.to_excel(out_file, index=False)
    return out_file

# ── Головна функція ───────────────────────────────────────────
def main():
    start_time = time.time()
    parse_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    df = collect_catalog()
    if df.empty:
        print("❌ Не вдалося зібрати каталог навіть через безкоштовні проксі. Спробуйте локальний запуск.")
        return
        
    df = enrich_with_details(df)
    out_file = clean_and_save(df, parse_date)
    
    print(f"\n✅ Успішно завершено за {time.time() - start_time:.1f} сек. Результат у {out_file}")
    print(f"Статистика -> Блокувань 403: {STATS.get('403', 0)} | Помилок зв'язку/проксі: {STATS.get('errors', 0)}")

if __name__ == "__main__":
    main()
