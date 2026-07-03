import concurrent.futures
import time
import random
import threading
from collections import deque
from datetime import datetime
from typing import Optional, List, Dict, Set
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Налаштування ──────────────────────────────────────────────
BASE_URL = "https://growex.market"

# Оновлений список унікальних категорій
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
    "ЗЗР (загальний)":      "/products/zasobi-zahistu-roslin-zzr",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8",
    "Referer": "https://growex.market/",
}

MAX_CATALOG_WORKERS = 8
MAX_DETAIL_WORKERS  = 10
MAX_EMPTY_PAGES     = 2
RETRY_DELAYS        = [2, 5, 10]
MIN_DELAY           = 0.2
MAX_DELAY           = 0.7

# ── Потокобезпечна сесія (одна на потік) ─────────────────────
thread_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        thread_local.session = s
    return thread_local.session

def throttle():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

# ── Базовий GET з retry ───────────────────────────────────────
def safe_get(url: str, retries: int = 3) -> Optional[requests.Response]:
    session = get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                print(f"  ⚠ 403 → {url} | спроба {attempt+1}/{retries} | чекаємо {wait}с")
                time.sleep(wait)
                continue
            if resp.status_code == 429:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] * 2
                print(f"  ⚠ 429 Too Many Requests → чекаємо {wait}с")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            print(f"  ⚠ Помилка: {e} | спроба {attempt+1}/{retries} | чекаємо {wait}с")
            time.sleep(wait)
    print(f"  ✗ Всі спроби вичерпані → {url}")
    return None


# ═══════════════════════════════════════════════════════════════
# КРОК 0: BFS-виявлення підкатегорій
# ═══════════════════════════════════════════════════════════════

def get_subcategories(path: str) -> List[str]:
    url = f"{BASE_URL}{path}"
    resp = safe_get(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    found = set()

    for a in soup.select("nav a[href], .sidebar a[href], .categories a[href], .filter a[href]"):
        href = a.get("href", "")
        if href.startswith("/products/") and href != path:
            found.add(href.split("?")[0])

    for a in soup.select("a[href*='/products/']"):
        href = a.get("href", "")
        if href.startswith("/products/") and href != path and "/product/" not in href:
            found.add(href.split("?")[0])

    return list(found)


def discover_all_categories(base_categories: Dict[str, str]) -> Dict[str, str]:
    visited: Set[str] = set(base_categories.values())
    all_cats: Dict[str, str] = dict(base_categories)
    queue: deque = deque(base_categories.items())

    print("=== Крок 0: виявлення підкатегорій (BFS) ===")

    while queue:
        parent_name, parent_path = queue.popleft()
        subcats = get_subcategories(parent_path)
        throttle()

        for sub_path in subcats:
            if sub_path not in visited:
                visited.add(sub_path)
                sub_slug = sub_path.split("/")[-1]
                sub_name = f"{parent_name} → {sub_slug}"
                all_cats[sub_name] = sub_path
                queue.append((sub_name, sub_path))
                print(f"  + {sub_name}")

    added = len(all_cats) - len(base_categories)
    print(f"Базових категорій: {len(base_categories)} | "
          f"Знайдено підкатегорій: {added} | "
          f"Всього: {len(all_cats)}")
    return all_cats


# ═══════════════════════════════════════════════════════════════
# КРОК 1: Збір карток товарів
# ═══════════════════════════════════════════════════════════════

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


def load_category(category_name: str, url_base: str) -> List[Dict]:
    all_rows = []
    empty_streak = 0
    page = 1

    while True:
        url = f"{BASE_URL}{url_base}?page={page}"
        resp = safe_get(url)
        throttle()

        if resp is None:
            break

        soup = BeautifulSoup(resp.text, "lxml")
        rows = parse_cards(soup, category_name, page)

        if not rows:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES:
                break
        else:
            empty_streak = 0
            all_rows.extend(rows)

        page += 1

    print(f"  [{category_name}] → {len(all_rows)} товарів ({page-1} стор.)")
    return all_rows


def collect_catalog(all_categories: Dict[str, str]) -> pd.DataFrame:
    print(f"\n=== Крок 1: збір каталогу ({len(all_categories)} категорій, "
          f"{MAX_CATALOG_WORKERS} потоків) ===")

    all_rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CATALOG_WORKERS) as ex:
        futures = {
            ex.submit(load_category, name, path): name
            for name, path in all_categories.items()
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                all_rows.extend(future.result())
            except Exception as e:
                print(f"  ✗ Помилка категорії [{futures[future]}]: {e}")

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    before = len(df)
    df = df.drop_duplicates(subset=["URL"]).reset_index(drop=True)
    print(f"\nЗібрано: {before} | Унікальних: {len(df)} | "
          f"Дублів видалено: {before - len(df)}")
    return df


# ═══════════════════════════════════════════════════════════════
# КРОК 2: Деталі товарів
# ═══════════════════════════════════════════════════════════════

def get_details(item: Dict, retries: int = 3) -> Dict:
    url = item["URL"]
    session = get_session()

    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            brand_tag   = soup.select_one("a[href*='/brand/']")
            price_l_tag = soup.select_one("div.one_price")

            item["Виробник"]     = brand_tag.get_text(strip=True)   if brand_tag   else ""
            item["Ціна_за_літр"] = price_l_tag.get_text(strip=True) if price_l_tag else ""
            throttle()
            return item

        except Exception:
            time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])

    item["Виробник"]     = ""
    item["Ціна_за_літр"] = ""
    return item


def enrich_with_details(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n=== Крок 2: деталі товарів ({len(df)} шт., "
          f"{MAX_DETAIL_WORKERS} потоків) ===")

    records = df.to_dict("records")
    enriched = []
    done = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DETAIL_WORKERS) as ex:
        futures = [ex.submit(get_details, item) for item in records]

        for future in concurrent.futures.as_completed(futures):
            done += 1
            if done % 100 == 0 or done == len(records):
                print(f"  Оброблено: {done}/{len(records)}")
            try:
                enriched.append(future.result())
            except Exception as e:
                print(f"  ✗ Помилка деталей: {e}")

    return pd.DataFrame(enriched)


# ═══════════════════════════════════════════════════════════════
# ФІНАЛЬНА ОБРОБКА ТА ЗБЕРЕЖЕННЯ
# ═══════════════════════════════════════════════════════════════

def clean_and_save(df: pd.DataFrame, parse_date: str) -> tuple[pd.DataFrame, str]:
    df["Ціна"] = (
        df["Ціна"]
        .str.replace("грн.", "", regex=False)
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )
    for col in ["Назва", "Ціна_за_літр", "Виробник"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df["Дата_парсінгу"] = parse_date

    cols = ["Назва", "Ціна", "Ціна_за_літр", "Виробник",
            "Категорія", "URL", "Сторінка", "Дата_парсінгу"]
    df = df[[c for c in cols if c in df.columns]]

    out_file = "growex_zzr.xlsx"
    df.to_excel(out_file, index=False)
    return df, out_file


# ═══════════════════════════════════════════════════════════════
# ТОЧКА ВХОДУ
# ═══════════════════════════════════════════════════════════════

def main() -> pd.DataFrame:
    start_time = time.time()
    parse_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    all_categories = discover_all_categories(CATEGORIES)

    df = collect_catalog(all_categories)
    if df.empty:
        print("❌ Каталог порожній — перевір з'єднання або селектори.")
        return df

    df = enrich_with_details(df)

    df, out_file = clean_and_save(df, parse_date)

    elapsed = time.time() - start_time
    print(f"\n✅ Збережено {len(df)} рядків → {out_file}")
    print(f"⏱  Час: {elapsed:.1f} сек. ({elapsed/60:.1f} хв.)\n")

    print("Зведення по категоріях:")
    summary = (
        df.groupby("Категорія")
        .size()
        .reset_index(name="Кількість")
        .sort_values("Кількість", ascending=False)
    )
    print(summary.to_string(index=False))

    return df


if __name__ == "__main__":
    df = main()
