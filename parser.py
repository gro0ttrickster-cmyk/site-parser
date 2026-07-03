import concurrent.futures, time, random, threading, re, pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
from curl_cffi import requests

BASE_URL = "https://growex.market"

# Змінено порядок: ЗЗР (загальний) тепер в самому кінці списку
CATEGORIES = {
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
    "ЗЗР (загальний)":      "/products/zasobi-zahistu-roslin-zzr", # <--- Тепер тут
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9", "Referer": BASE_URL + "/"
}

MAX_WORKERS, RETRY_DELAYS = 8, [2, 4, 8]
STATS = {"403": 0, "errors": 0}
_stats_lock, thread_local = threading.Lock(), threading.local()

def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        s = requests.Session(impersonate="chrome120")
        s.headers.update(HEADERS)
        thread_local.session = s
    return thread_local.session

def safe_get(url: str, retries: int = len(RETRY_DELAYS)) -> Optional[str]:
    session = get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 200: return resp.text
            if resp.status_code == 404: return None
            if resp.status_code in [403, 429]:
                with _stats_lock: STATS["403"] += 1
                time.sleep(RETRY_DELAYS[attempt] + random.uniform(0.5, 1.5))
        except Exception:
            with _stats_lock: STATS["errors"] += 1
            time.sleep(RETRY_DELAYS[attempt])
    return None

def get_max_pages(category_path: str) -> int:
    html = safe_get(f"{BASE_URL}{category_path}?page=1")
    if not html: return 1
    soup = BeautifulSoup(html, "lxml")
    pages = []
    for a in soup.select("a[href*='page=']"):
        match = re.search(r'page=(\d+)', a.get("href", ""))
        if match: pages.append(int(match.group(1)))
    return max(pages) if pages else 1

def load_page(category_name: str, url_base: str, page: int) -> List[Dict]:
    html = safe_get(f"{BASE_URL}{url_base}?page={page}")
    if not html: return []
    soup = BeautifulSoup(html, "lxml")
    return [{
        "Назва": a.get_text(strip=True),
        "Ціна": card.select_one("div.card_product-price").get_text(strip=True) if card.select_one("div.card_product-price") else "",
        "URL": a.get("href") if a.get("href", "").startswith("http") else BASE_URL + a.get("href", ""),
        "Категорія": category_name, "Сторінка": page
    } for card in soup.select(".card_product") if (a := card.select_one("a[href*='/product/']"))]

def get_details(item: Dict) -> Dict:
    html = safe_get(item["URL"])
    if html:
        soup = BeautifulSoup(html, "lxml")
        b, p = soup.select_one("a[href*='/brand/']"), soup.select_one("div.one_price")
        item["Виробник"] = b.get_text(strip=True) if b else ""
        item["Ціна_за_літр"] = p.get_text(strip=True) if p else ""
    else: item["Виробник"], item["Ціна_за_літр"] = "", ""
    return item

def main():
    start_time = time.time()
    print("🔎 Аналіз сторінок у категоріях...")
    cat_pages = {n: get_max_pages(p) for n, p in CATEGORIES.items()}
    for n, p in cat_pages.items(): print(f"  -> {n}: знайдено сторінок — {p}")

    all_rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(load_page, n, path, p) for n, path in CATEGORIES.items() for p in range(1, cat_pages[n] + 1)]
        for f in concurrent.futures.as_completed(futures):
            if res := f.result(): all_rows.extend(res)

    if not all_rows:
        print("❌ Не вдалося зібрати каталог. Спробуйте запустити код локально на ПК.")
        return

    df = pd.DataFrame(all_rows)
    df['is_gen'] = df['Категорія'] == "ЗЗР (загальний)"
    df = df.sort_values(by='is_gen').drop(columns=['is_gen']).drop_duplicates(subset=["URL"]).reset_index(drop=True)
    print(f"📦 Збір деталей для {len(df)} товарів...")

    enriched = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(get_details, item) for item in df.to_dict("records")]
        for i, f in enumerate(concurrent.futures.as_completed(futures), 1):
            enriched.append(f.result())
            if i % 100 == 0 or i == len(df): print(f"  Прогрес: {i}/{len(df)}")

    df = pd.DataFrame(enriched)
    df["Ціна"] = df["Ціна"].astype(str).str.replace("грн.", "", regex=False).str.strip()
    df["Дата_парсінгу"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    cols = ["Назва", "Ціна", "Ціна_за_літр", "Виробник", "Категорія", "URL", "Сторінка", "Дата_парсінгу"]
    df[[c for c in cols if c in df.columns]].to_excel("growex_zzr.xlsx", index=False)
    
    print(f"\n✅ Успішно завершено за {time.time() - start_time:.1f}с. Блокувань: {STATS['403']}")

if __name__ == "__main__":
    main()
