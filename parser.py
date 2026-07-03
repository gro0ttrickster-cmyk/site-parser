from __future__ import annotations

import argparse
import concurrent.futures
import logging
import random
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests

# --------------------------------------------------------------------------- #
# Конфігурація
# --------------------------------------------------------------------------- #

BASE_URL = "https://growex.market"

CATEGORIES: Dict[str, str] = {
    "Гербіциди": "/products/gerbicidi",
    "Інсектициди": "/products/insekticidi",
    "Акарициди": "/products/akaricidi",
    "Родентициди": "/products/rodenticidi",
    "Протруйники насіння": "/products/protruyniki-nasinnya",
    "Фунгіциди": "/products/fungicidi",
    "Інокулянти": "/products/inokulyanti",
    "Регулятори росту": "/products/regulyatori-rostu",
    "Допоміжні засоби": "/products/dopomizhni-zasobi",
    "Фуміганти": "/products/fumiganti",
    "Біологічні препарати": "/products/biologichni-preparati",
    "Пакети для фермера": "/products/paketi-dlya-fermera",
    "Антисептики": "/products/antiseptiki",
    "ЗЗР (загальний)": "/products/zasobi-zahistu-roslin-zzr",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9",
    "Referer": BASE_URL + "/",
}

RETRY_DELAYS = (4, 8, 15, 25)
DEFAULT_WORKERS = 3  # Зменшено за замовчуванням для стабільності
DEFAULT_OUTPUT = "growex_zzr.xlsx"
SECOND_PASS_DELAY = 7.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("growex")


@dataclass
class Stats:
    blocked: int = 0
    errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def inc_blocked(self) -> None:
        with self.lock:
            self.blocked += 1

    def inc_error(self) -> None:
        with self.lock:
            self.errors += 1


@dataclass
class FailedPage:
    category_name: str
    url_base: str
    page: int


STATS = Stats()
_thread_local = threading.local()
GLOBAL_COOKIES = {}  # Спільні базові куки після розігріву


# --------------------------------------------------------------------------- #
# HTTP-шар
# --------------------------------------------------------------------------- #

def warm_up_session() -> None:
    """Робить тестовий запит до головної сторінки для ініціалізації куків."""
    global GLOBAL_COOKIES
    log.info("Розігрів сесії (отримання базових куків сайту)...")
    try:
        with requests.Session(impersonate="chrome124") as s:
            s.headers.update(HEADERS)
            resp = s.get(BASE_URL, timeout=15)
            if resp.status_code == 200:
                GLOBAL_COOKIES = resp.cookies.get_dict()
                log.info("Сесію успішно розігріто.")
            else:
                log.warning("Розігрів повернув статус %d. Працюємо без базових куків.", resp.status_code)
    except Exception as e:
        log.warning("Не вдалося розігріти сесію: %s. Можливі блокування.", e)


def get_session() -> requests.Session:
    """Одна сесія на потік з імпортованими базовими куками."""
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session(impersonate="chrome124")
        session.headers.update(HEADERS)
        if GLOBAL_COOKIES:
            session.cookies.update(GLOBAL_COOKIES)
        _thread_local.session = session
    return session


class PageBlockedError(Exception):
    """Сторінка заблокована сайтом."""


def safe_get(url: str, retries: int = len(RETRY_DELAYS)) -> Optional[str]:
    session = get_session()
    last_status: Optional[int] = None
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            last_status = resp.status_code
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 404:
                return None
            if resp.status_code in (403, 429):
                STATS.inc_blocked()
                delay = RETRY_DELAYS[attempt] + random.uniform(1.0, 3.0)
                log.debug("HTTP %s на %s, повтор через %.1fс", resp.status_code, url, delay)
                time.sleep(delay)
        except Exception as exc:
            STATS.inc_error()
            last_status = None
            log.debug("Помилка запиту %s: %s", url, exc)
            time.sleep(RETRY_DELAYS[attempt] + random.uniform(0.5, 1.5))
            
    log.warning(
        "Не вдалося отримати сторінку після %d спроб (останній статус: %s): %s",
        retries, last_status if last_status is not None else "мережева помилка", url,
    )
    raise PageBlockedError(url)


# --------------------------------------------------------------------------- #
# Парсинг
# --------------------------------------------------------------------------- #

def get_max_pages(category_path: str) -> int:
    try:
        # Штучна пауза, щоб потоки не атакували сайт одночасно
        time.sleep(random.uniform(1.0, 3.5))
        html = safe_get(f"{BASE_URL}{category_path}?page=1")
    except PageBlockedError:
        log.error("Не вдалося визначити кількість сторінок для %s — сайт заблокував запит (403/429)", category_path)
        return 1
    if not html:
        return 1
    soup = BeautifulSoup(html, "lxml")
    pages = [
        int(m.group(1))
        for a in soup.select("a[href*='page=']")
        if (m := re.search(r"page=(\d+)", a.get("href", "")))
    ]
    return max(pages) if pages else 1


def load_page(category_name: str, url_base: str, page: int) -> tuple[List[Dict], bool]:
    try:
        html = safe_get(f"{BASE_URL}{url_base}?page={page}")
    except PageBlockedError:
        return [], True
    if not html:
        return [], False
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for card in soup.select(".card_product"):
        link = card.select_one("a[href*='/product/']")
        if not link:
            continue
        href = link.get("href", "")
        price_el = card.select_one("div.card_product-price")
        rows.append({
            "Назва": link.get_text(strip=True),
            "Ціна": price_el.get_text(strip=True) if price_el else "",
            "URL": href if href.startswith("http") else BASE_URL + href,
            "Категорія": category_name,
            "Сторінка": page,
        })
    return rows, False


def get_details(item: Dict) -> Dict:
    try:
        # Затримка між заходами в картки товарів
        time.sleep(random.uniform(0.3, 1.2))
        html = safe_get(item["URL"])
    except PageBlockedError:
        html = None
    if html:
        soup = BeautifulSoup(html, "lxml")
        brand = soup.select_one("a[href*='/brand/']")
        price_per_l = soup.select_one("div.one_price")
        item["Виробник"] = brand.get_text(strip=True) if brand else ""
        item["Ціна_за_літр"] = price_per_l.get_text(strip=True) if price_per_l else ""
    else:
        item["Виробник"], item["Ціна_за_літр"] = "", ""
    return item


# --------------------------------------------------------------------------- #
# Оркестрація
# --------------------------------------------------------------------------- #

def collect_catalog(categories: Dict[str, str], workers: int) -> pd.DataFrame:
    warm_up_session()
    
    log.info("Аналіз кількості сторінок у %d категоріях...", len(categories))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(get_max_pages, path): name for name, path in categories.items()}
        cat_pages = {}
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            cat_pages[name] = fut.result()
            log.info("  %s: сторінок — %d", name, cat_pages[name])

    all_rows: List[Dict] = []
    failed_pages: List[FailedPage] = []
    
    log.info("Збір списків товарів з категорій...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        future_map = {
            ex.submit(load_page, name, path, page): FailedPage(name, path, page)
            for name, path in categories.items()
            for page in range(1, cat_pages[name] + 1)
        }
        for fut in concurrent.futures.as_completed(future_map):
            rows, failed = fut.result()
            if failed:
                failed_pages.append(future_map[fut])
            elif rows:
                all_rows.extend(rows)

    if failed_pages:
        log.warning(
            "%d сторінок не завантажились паралельно — повторюю серійно через %.0fс...",
            len(failed_pages), SECOND_PASS_DELAY,
        )
        time.sleep(SECOND_PASS_DELAY)
        still_failed: List[FailedPage] = []
        for fp in failed_pages:
            try:
                rows, failed = load_page(fp.category_name, fp.url_base, fp.page)
            except Exception as exc:
                log.debug("Другий прохід також невдалий для %s ст.%d: %s", fp.category_name, fp.page, exc)
                rows, failed = [], True
            if failed:
                still_failed.append(fp)
            elif rows:
                all_rows.extend(rows)
            time.sleep(random.uniform(1.5, 3.0))

        if still_failed:
            log.error(
                "Остаточно не вдалось завантажити %d сторінок (можлива втрата товарів):",
                len(still_failed),
            )
            for fp in still_failed:
                log.error("  %s (сторінка %d): %s%s?page=%d", fp.category_name, fp.page, BASE_URL, fp.url_base, fp.page)
        else:
            log.info("Другий прохід відновив усі раніше невдалі сторінки.")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["_is_general"] = df["Категорія"] == "ЗЗР (загальний)"
    df = (
        df.sort_values(by="_is_general")
        .drop(columns="_is_general")
        .drop_duplicates(subset="URL", keep="first")
        .reset_index(drop=True)
    )
    return df


def enrich_with_details(df: pd.DataFrame, workers: int) -> pd.DataFrame:
    log.info("Збір деталей для %d товарів...", len(df))
    enriched: List[Dict] = []
    total = len(df)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(get_details, item) for item in df.to_dict("records")]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            enriched.append(fut.result())
            if i % 50 == 0 or i == total:
                log.info("  Прогрес: %d/%d", i, total)
    return pd.DataFrame(enriched)


def save_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    df["Ціна"] = df["Ціна"].astype(str).str.replace("грн.", "", regex=False).str.strip()
    df["Дата_парсінгу"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    cols = ["Назва", "Ціна", "Ціна_за_літр", "Виробник", "Категорія", "URL", "Сторінка", "Дата_парсінгу"]
    cols = [c for c in cols if c in df.columns]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df[cols].to_excel(output_path, index=False)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Парсер каталогу growex.market")
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Кількість потоків (за замовчуванням {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--output", type=Path, default=Path(DEFAULT_OUTPUT),
        help=f"Шлях до вихідного .xlsx файлу (за замовчуванням {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--categories", type=str, default=None,
        help="Список категорій через кому (за замовчуванням — усі). "
             "Приклад: \"Гербіциди,Фунгіциди\"",
    )
    parser.add_argument(
        "--no-details", action="store_true",
        help="Пропустити збір сторінок окремих товарів (виробник, ціна за літр) — швидше",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Детальне логування (DEBUG)",
    )
    return parser.parse_args(argv)


def resolve_categories(names_csv: Optional[str]) -> Dict[str, str]:
    if not names_csv:
        return CATEGORIES
    requested = [n.strip() for n in names_csv.split(",") if n.strip()]
    unknown = [n for n in requested if n not in CATEGORIES]
    if unknown:
        log.error("Невідомі категорії: %s", ", ".join(unknown))
        log.error("Доступні: %s", ", ".join(CATEGORIES))
        sys.exit(1)
    return {n: CATEGORIES[n] for n in requested}


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    if args.verbose:
        log.setLevel(logging.DEBUG)

    categories = resolve_categories(args.categories)
    start = time.time()

    df = collect_catalog(categories, args.workers)
    if df.empty:
        log.error("Не вдалося зібрати каталог. Перевірте мережу / доступність сайту.")
        sys.exit(1)

    log.info("Знайдено %d унікальних товарів.", len(df))

    if not args.no_details:
        df = enrich_with_details(df, args.workers)

    save_to_excel(df, args.output)

    elapsed = time.time() - start
    log.info(
        "✅ Готово за %.1fс → %s (блокувань 403/429: %d, помилок мережі: %d)",
        elapsed, args.output, STATS.blocked, STATS.errors,
    )


if __name__ == "__main__":
    main()
