import concurrent.futures
import time
import random
import threading
from collections import deque
from datetime import datetime
from typing import Optional, List, Dict, Set, Tuple
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

# ── Налаштування ──────────────────────────────────────────────
BASE_URL = "https://growex.market"

CATEGORIES = {
    "ЗЗР (загальний)":     "/products/zasobi-zahistu-roslin-zzr",
    "Гербіциди":            "/products/gerbicidi",
    "Десиканти":            "/products/desikanti-2",
    "Інсектициди":          "/products/insekticidi",
    "Акарициди":            "/products/akaricidi",
    "Родентициди":          "/products/rodenticidi",
    "Протруйники насіння":  "/products/protruyniki-nasinnya",
    "Фунгіциди":            "/products/fungicidi",
    "Інокулянти":           "/products/inokulyanti",
    "Регулятори росту":     "/products/regulyatori-rostu",
    "Допоміжні засоби":     "/products/dopomizhni-zasobi",
    "Біологічні препарати": "/products/biologichni-preparati",
    "Фуміганти":            "/products/fumiganti",
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

MAX_CATALOG_WORKERS   = 6
MAX_DETAIL_WORKERS    = 8
MAX_EMPTY_PAGES       = 2
MAX_PAGES_PER_CAT     = 300   # запобіжник від нескінченного циклу пагінації
MAX_CONSECUTIVE_FAILS = 5     # скільки поспіль ПОВНІСТЮ провалених сторінок (не порожніх!)
                               # терпимо, перш ніж здатись і перейти до наступної категорії
RETRY_DELAYS          = [3, 6, 12, 20, 30]

# Базовий інтервал між запитами — ОКРЕМИЙ для кожної фази, бо листинг
# категорій (де й ловили 429) і сторінки окремих товарів — різні
# ендпоінти, і немає причин тримати їх на однаковому, дуже обережному
# темпі. Якщо на фазі "деталі" 429 все ж з'явиться — адаптивний backoff
# нижче сам пригальмує, як і раніше.
PHASE_INTERVALS = {
    "catalog": 0.45,   # ≈ 2.2 запити/сек — перевірено, не ловить 429
    "details": 0.03,   # ≈ 33 запити/сек — на цій фазі 0 разів впіймали 429 навіть
                        # на 0.15с, а сервер відповідає за ~0.4с; backoff
                        # нижче сам підніме інтервал, якщо ліміт таки є
}
JITTER_MAX = 0.25

# ── Діагностика мережевих затримок ────────────────────────────
_stats_lock = threading.Lock()
STATS = {
    "403": 0, "429": 0, "errors": 0, "retry_seconds": 0.0,
    "other_status": 0, "exceptions": 0,
    "latency_sum": 0.0, "latency_count": 0,
}

def _record(kind: str, wait: float = 0.0):
    with _stats_lock:
        STATS[kind] += 1
        STATS["retry_seconds"] += wait

def _record_latency(seconds: float):
    with _stats_lock:
        STATS["latency_sum"] += seconds
        STATS["latency_count"] += 1

# ── Глобальний rate-gate + адаптивний backoff ─────────────────
# Коли сайт починає відповідати 429, інтервал між запитами
# зростає для УСІХ потоків одночасно (а не для того, що спіймав
# 429). Штраф поступово спадає під час успішних запитів.
_gate_lock = threading.Lock()
_next_slot = 0.0
_global_backoff = 0.0
_current_phase = "catalog"
BACKOFF_STEP  = 1.0    # наскільки зростає штраф на кожен 429
BACKOFF_MAX   = 10.0
BACKOFF_DECAY = 0.1    # наскільки спадає штраф на кожен успішний запит

def set_phase(phase: str):
    """Перемикає базовий інтервал і скидає backoff на початку нової фази
    (крок 1 → крок 2), бо це різні ендпоінти з різною толерантністю сайту."""
    global _current_phase, _global_backoff
    with _gate_lock:
        _current_phase = phase
        _global_backoff = 0.0

def _note_rate_limited():
    global _global_backoff
    with _gate_lock:
        _global_backoff = min(_global_backoff + BACKOFF_STEP, BACKOFF_MAX)

def _note_success():
    global _global_backoff
    if _global_backoff > 0:
        with _gate_lock:
            _global_backoff = max(0.0, _global_backoff - BACKOFF_DECAY)

def rate_gate():
    """Пропускає виклики з усіх потоків по черзі не частіше базового
    інтервалу поточної фази + поточний backoff."""
    global _next_slot
    with _gate_lock:
        now = time.time()
        start = max(now, _next_slot)
        base = PHASE_INTERVALS.get(_current_phase, 0.45)
        interval = base + _global_backoff + random.uniform(0, JITTER_MAX)
        _next_slot = start + interval
        wait = start - now
    if wait > 0:
        time.sleep(wait)

# ── Потокобезпечна сесія (одна на потік) ─────────────────────
thread_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        # Пул з'єднань під кількість воркерів, щоб уникнути
        # "Connection pool is full" і зайвих переоткриттів TCP
        adapter = HTTPAdapter(
            pool_connections=max(MAX_CATALOG_WORKERS, MAX_DETAIL_WORKERS),
            pool_maxsize=max(MAX_CATALOG_WORKERS, MAX_DETAIL_WORKERS),
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        thread_local.session = s
    return thread_local.session

# ── Базовий GET з retry ───────────────────────────────────────
def safe_get(url: str, retries: int = len(RETRY_DELAYS)) -> Optional[requests.Response]:
    session = get_session()
    for attempt in range(retries):
        rate_gate()
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                print(f"  ⚠ 403 → {url} | спроба {attempt+1}/{retries} | чекаємо {wait}с")
                _record("403", wait)
                _note_rate_limited()
                time.sleep(wait)
                continue
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.strip().isdigit() and int(retry_after) > 0:
                    wait = float(retry_after)
                else:
                    wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] * 2
                print(f"  ⚠ 429 Too Many Requests → чекаємо {wait}с")
                _record("429", wait)
                _note_rate_limited()
                time.sleep(wait)
                continue
            resp.raise_for_status()
            _note_success()
            return resp
        except requests.RequestException as e:
            wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            print(f"  ⚠ Помилка: {e} | спроба {attempt+1}/{retries} | чекаємо {wait}с")
            _record("errors", wait)
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
    all_rows: List[Dict] = []
    seen_urls: Set[str] = set()
    empty_streak = 0
    fail_streak = 0
    page = 1

    while page <= MAX_PAGES_PER_CAT:
        url = f"{BASE_URL}{url_base}?page={page}"
        resp = safe_get(url)

        if resp is None:
            # Повний фейл сторінки (усі ретраї вичерпані) — це НЕ те саме,
            # що "товарів більше немає". Пропускаємо сторінку і пробуємо
            # далі, інакше один тимчасовий збій обрізає всю категорію.
            fail_streak += 1
            print(f"  ⚠ [{category_name}] стор. {page} недоступна "
                  f"({fail_streak}/{MAX_CONSECUTIVE_FAILS}) — пробую далі")
            if fail_streak >= MAX_CONSECUTIVE_FAILS:
                print(f"  ✗ [{category_name}] забагато послідовних збоїв — зупиняюсь")
                break
            page += 1
            continue

        fail_streak = 0
        soup = BeautifulSoup(resp.text, "lxml")
        rows = parse_cards(soup, category_name, page)

        # Рахуємо тільки ще не бачені URL — так ловимо і порожні
        # сторінки, і сайти, що на "page=N" за межами діапазону
        # просто повертають останню сторінку знову (без цього
        # цикл міг би крутитись нескінченно)
        new_rows = [r for r in rows if r["URL"] not in seen_urls]

        if not new_rows:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES:
                break
        else:
            empty_streak = 0
            for r in new_rows:
                seen_urls.add(r["URL"])
            all_rows.extend(new_rows)

        page += 1

    if page > MAX_PAGES_PER_CAT:
        print(f"  ⚠ [{category_name}] досягнуто ліміт {MAX_PAGES_PER_CAT} сторінок — зупинено")

    print(f"  [{category_name}] → {len(all_rows)} товарів ({page-1} стор.)")
    return all_rows


def collect_catalog(scrape_categories: Dict[str, str]) -> pd.DataFrame:
    print(f"\n=== Крок 1: збір каталогу ({len(scrape_categories)} категорій, "
          f"{MAX_CATALOG_WORKERS} потоків) ===")

    all_rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CATALOG_WORKERS) as ex:
        futures = {
            ex.submit(load_category, name, path): name
            for name, path in scrape_categories.items()
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

def get_details(item: Dict, retries: int = len(RETRY_DELAYS)) -> Dict:
    url = item["URL"]
    session = get_session()

    for attempt in range(retries):
        rate_gate()
        t_req0 = time.time()
        try:
            resp = session.get(url, timeout=15)
            _record_latency(time.time() - t_req0)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.strip().isdigit() and int(retry_after) > 0:
                    wait = float(retry_after)
                else:
                    wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)] * 2
                _record("429", wait)
                _note_rate_limited()
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                _record("other_status", wait)
                print(f"  ⚠ Статус {resp.status_code} → {url} | спроба {attempt+1}/{retries}")
                time.sleep(wait)
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            brand_tag   = soup.select_one("a[href*='/brand/']")
            price_l_tag = soup.select_one("div.one_price")

            item["Виробник"]     = brand_tag.get_text(strip=True)   if brand_tag   else ""
            item["Ціна_за_літр"] = price_l_tag.get_text(strip=True) if price_l_tag else ""
            _note_success()
            return item

        except Exception as e:
            wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            _record("exceptions", wait)
            print(f"  ⚠ Виняток: {e} → {url} | спроба {attempt+1}/{retries}")
            time.sleep(wait)

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

def clean_and_save(df: pd.DataFrame, parse_date: str) -> Tuple[pd.DataFrame, str]:
    df["Ціна"] = (
        df["Ціна"]
        .astype(str)
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

    t0 = time.time()
    all_categories = discover_all_categories(CATEGORIES)
    t_discovery = time.time() - t0

    # Скануємо ВСІ категорії, включно з "ЗЗР (загальний)": вона містить
    # частину товарів, не позначених жодною конкретною підкатегорією.
    # Перетини між категоріями прибираються нижче через drop_duplicates(URL).
    t0 = time.time()
    set_phase("catalog")
    df = collect_catalog(all_categories)
    t_catalog = time.time() - t0

    if df.empty:
        print("❌ Каталог порожній — перевір з'єднання або селектори.")
        return df

    t0 = time.time()
    set_phase("details")
    df = enrich_with_details(df)
    t_details = time.time() - t0

    df, out_file = clean_and_save(df, parse_date)

    elapsed = time.time() - start_time
    print(f"\n✅ Збережено {len(df)} рядків → {out_file}")
    print(f"⏱  Час: {elapsed:.1f} сек. ({elapsed/60:.1f} хв.)\n")

    print("Таймінг по етапах:")
    print(f"  Крок 0 (discovery):   {t_discovery:6.1f} сек.")
    print(f"  Крок 1 (каталог):     {t_catalog:6.1f} сек.")
    print(f"  Крок 2 (деталі):      {t_details:6.1f} сек.")

    print("\nМережева діагностика (ретраї, що з'їдають час поза видимим прогресом):")
    print(f"  403 Forbidden:          {STATS['403']}")
    print(f"  429 Too Many Requests:  {STATS['429']}")
    print(f"  Інші статуси (не 200):  {STATS['other_status']}")
    print(f"  Винятки/таймаути:       {STATS['exceptions']}")
    print(f"  Сумарний час чекання на ретраях: {STATS['retry_seconds']:.1f} сек.")
    if STATS["latency_count"]:
        avg_latency = STATS["latency_sum"] / STATS["latency_count"]
        print(f"  Середній час відповіді сервера (деталі): {avg_latency:.2f} сек. "
              f"({STATS['latency_count']} запитів)")

    print("\nЗведення по категоріях:")
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
