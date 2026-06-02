import concurrent.futures
import time
from datetime import datetime
from typing import Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Налаштування ──────────────────────────────────────────────
BASE_URL = "https://growex.market"
CATALOG_URL = BASE_URL + "/products/zasobi-zahistu-roslin-zzr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://growex.market/",
}

MAX_WORKERS = 3       # знижено щоб уникнути блокування
PAGE_DELAY = 0.7      # затримка між сторінками каталогу (сек)
RETRY_DELAYS = [5, 10, 20]  # затримки між повторними спробами при 403


# ── Крок 1: завантаження однієї сторінки каталогу ─────────────
def load_page(session: requests.Session, page: int, retries: int = 3) -> Optional[list]:
    url = f"{CATALOG_URL}?page={page}"

    for attempt in range(retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=15)

            if resp.status_code == 403:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                print(f"  ⚠ 403 на сторінці {page}, спроба {attempt + 1}/{retries}, чекаємо {wait}с...")
                time.sleep(wait)
                continue

            if resp.status_code == 404:
                print(f"  → 404 на сторінці {page} — кінець каталогу.")
                return None

            resp.raise_for_status()

        except requests.RequestException as e:
            wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            print(f"  ⚠ Помилка на сторінці {page}: {e}, спроба {attempt + 1}/{retries}, чекаємо {wait}с...")
            time.sleep(wait)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select(".card_product")

        if not cards:
            print(f"  → Сторінка {page}: карток не знайдено — кінець каталогу.")
            return None

        rows = []
        for card in cards:
            a_tag = card.select_one("a[href*='/product/']")
            name = a_tag.get_text(strip=True) if a_tag else ""
            href = a_tag["href"] if a_tag and a_tag.get("href") else ""
            full_url = href if href.startswith("http") else BASE_URL + href

            price_tag = card.select_one("div.card_product-price")
            price = price_tag.get_text(strip=True) if price_tag else ""

            rows.append({"Назва": name, "Ціна": price, "URL": full_url, "Сторінка": page})

        return rows if rows else None

    # Всі спроби вичерпано
    print(f"  ✗ Сторінка {page}: всі {retries} спроби невдалі — пропускаємо.")
    return None


# ── Крок 1: обхід усіх сторінок каталогу ─────────────────────
def load_catalog(session: requests.Session) -> pd.DataFrame:
    all_rows = []
    page = 1

    while True:
        print(f"  Каталог — сторінка {page}…")
        rows = load_page(session, page)

        if rows is None:
            break

        all_rows.extend(rows)
        print(f"  ✓ Сторінка {page}: +{len(rows)} товарів (всього {len(all_rows)})")
        page += 1
        time.sleep(PAGE_DELAY)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["URL"])
    return df.reset_index(drop=True)


# ── Крок 2: деталі товару (виробник + ціна за літр) ───────────
# Окрема сесія на кожен потік щоб уникнути блокування
def get_details(url: str, retries: int = 3) -> dict:
    empty = {"Виробник": "", "Ціна_за_літр": "", "URL": url}

    for attempt in range(retries):
        try:
            with requests.Session() as s:
                resp = s.get(url, headers=HEADERS, timeout=15)

                if resp.status_code == 403:
                    wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                    time.sleep(wait)
                    continue

                if resp.status_code == 404:
                    return empty

                resp.raise_for_status()

        except requests.RequestException:
            wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            time.sleep(wait)
            continue

        soup = BeautifulSoup(resp.text, "lxml")

        brand_tag = soup.select_one("a[href*='/brand/']")
        brand = brand_tag.get_text(strip=True) if brand_tag else ""

        price_l_tag = soup.select_one("div.one_price")
        price_l = price_l_tag.get_text(strip=True) if price_l_tag else ""

        return {"Виробник": brand, "Ціна_за_літр": price_l, "URL": url}

    return empty


# ── Головна функція ───────────────────────────────────────────
def main() -> pd.DataFrame:
    start_time = time.time()
    parse_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    with requests.Session() as session:
        print("=== Крок 1: завантажуємо каталог ===")
        catalog = load_catalog(session)

        if catalog.empty:
            print("Каталог порожній — перевірте URL або з'єднання.")
            return catalog

        print(f"\nЗнайдено {len(catalog)} товарів. Починаємо збір деталей ({MAX_WORKERS} потоки)…")
        print("=== Крок 2: завантажуємо деталі товарів ===")

    # Деталі збираємо поза основною сесією — кожен потік відкриває свою
    urls = catalog["URL"].tolist()
    details_list = []
    counter = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(get_details, url): url for url in urls}

        for future in concurrent.futures.as_completed(future_to_url):
            counter += 1
            if counter % 20 == 0 or counter == len(urls):
                print(f"  Оброблено: {counter}/{len(urls)}")
            try:
                details_list.append(future.result())
            except Exception as e:
                url = future_to_url[future]
                print(f"  Помилка при обробці {url}: {e}")
                details_list.append({"Виробник": "", "Ціна_за_літр": "", "URL": url})

    details_df = pd.DataFrame(details_list)
    result = pd.merge(catalog, details_df, on="URL", how="left")

    # ── Очищення ──
    result["Ціна"] = (
        result["Ціна"]
        .str.replace("грн.", "", regex=False)
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )
    for col in ["Назва", "Ціна_за_літр", "Виробник"]:
        result[col] = result[col].astype(str).str.strip()

    result["Дата_парсінгу"] = parse_date
    result = result[["Назва", "Ціна", "Ціна_за_літр", "Виробник", "URL", "Сторінка", "Дата_парсінгу"]]

    # ── Зберігаємо ──
    out_file = "growex_zzr.xlsx"
    result.to_excel(out_file, index=False)

    elapsed = time.time() - start_time
    print(f"\n✅ Готово! Збережено {len(result)} рядків → {out_file}")
    print(f"Час виконання: {elapsed:.2f} сек.")
    return result


if __name__ == "__main__":
    df = main()
    print(df.head())
