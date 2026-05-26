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

MAX_WORKERS = 8


# ── Крок 1: завантаження однієї сторінки каталогу ─────────────
def load_page(session: requests.Session, page: int) -> Optional[list]:
    url = f"{CATALOG_URL}?page={page}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        if resp.status_code in (403, 404):
            return None
        resp.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    cards = soup.select(".card_product")
    if not cards:
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


# ── Крок 1: обхід усіх сторінок каталогу ─────────────────────
def load_catalog(session: requests.Session) -> pd.DataFrame:
    all_rows = []
    page = 1
    while True:
        print(f"  Каталог — сторінка {page}…")
        rows = load_page(session, page)
        if rows is None:
            print(f"  → Сторінка {page} порожня або недоступна. Зупинка.")
            break
        all_rows.extend(rows)
        page += 1
        time.sleep(0.2)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["URL"])
    return df.reset_index(drop=True)


# ── Крок 2: деталі товару (виробник + ціна за літр) ───────────
def get_details(session: requests.Session, url: str) -> dict:
    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        if resp.status_code in (403, 404):
            return {"Виробник": "", "Ціна_за_літр": "", "URL": url}
        resp.raise_for_status()
    except requests.RequestException:
        return {"Виробник": "", "Ціна_за_літр": "", "URL": url}

    soup = BeautifulSoup(resp.text, "lxml")

    brand_tag = soup.select_one("a[href*='/brand/']")
    brand = brand_tag.get_text(strip=True) if brand_tag else ""

    price_l_tag = soup.select_one("div.one_price")
    price_l = price_l_tag.get_text(strip=True) if price_l_tag else ""

    return {"Виробник": brand, "Ціна_за_літр": price_l, "URL": url}


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

        print(f"\nЗнайдено {len(catalog)} товарів. Починаємо багатопотоковий збір деталей…")
        print("=== Крок 2: завантажуємо деталі товарів (у декілька потоків) ===")

        urls = catalog["URL"].tolist()
        details_list = []
        counter = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {executor.submit(get_details, session, url): url for url in urls}

            for future in concurrent.futures.as_completed(future_to_url):
                counter += 1
                if counter % 10 == 0 or counter == len(urls):
                    print(f"  Оброблено товарів: {counter}/{len(urls)}")
                try:
                    data = future.result()
                    details_list.append(data)
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

    # ── Додаємо дату парсінгу ──
    result["Дата_парсінгу"] = parse_date

    result = result[["Назва", "Ціна", "Ціна_за_літр", "Виробник", "URL", "Дата_парсінгу"]]

    # ── Зберігаємо ──
    out_file = "growex_zzr.xlsx"
    result.to_excel(out_file, index=False)

    end_time = time.time()
    print(f"\n✅ Готово! Збережено {len(result)} рядків → {out_file}")
    print(f"Час виконання: {end_time - start_time:.2f} сек.")
    return result


if __name__ == "__main__":
    df = main()
    print(df.head())
