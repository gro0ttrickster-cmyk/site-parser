import time
from typing import Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Налаштування ──────────────────────────────────────────────
BASE_URL = "https://growex.market"
CATALOG_URL = BASE_URL + "/products/zasobi-zahistu-roslin-zzr"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer":         "https://growex.market/",
}

DELAY = 1.0  # секунди між запитами (щоб не перевантажувати сервер)


# ── Крок 1: завантаження однієї сторінки каталогу ─────────────
def load_page(page: int) -> Optional[list]:
    url = f"{CATALOG_URL}?page={page}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code in (403, 404):
            return None
        resp.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select(".card_product")
    if not cards:
        return None

    rows = []
    for card in cards:
        a_tag = card.select_one("a[href*='/product/']")
        name = a_tag.get_text(strip=True) if a_tag else ""
        href = a_tag["href"] if a_tag and a_tag.get("href") else ""
        # перетворюємо відносне посилання на абсолютне
        full_url = href if href.startswith("http") else BASE_URL + href

        price_tag = card.select_one("div.card_product-price")
        price = price_tag.get_text(strip=True) if price_tag else ""

        rows.append({"Назва": name, "Ціна": price, "URL": full_url, "Сторінка": page})

    return rows if rows else None


# ── Крок 1: обхід усіх сторінок каталогу ─────────────────────
def load_catalog() -> pd.DataFrame:
    all_rows = []
    page = 1
    while True:
        print(f"  Каталог — сторінка {page}…")
        rows = load_page(page)
        if rows is None:
            print(f"  → Сторінка {page} порожня або недоступна. Зупинка.")
            break
        all_rows.extend(rows)
        page += 1
        time.sleep(DELAY)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["URL"])
    return df.reset_index(drop=True)


# ── Крок 2: деталі товару (виробник + ціна за літр) ───────────
def get_details(url: str) -> dict:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code in (403, 404):
            return {"Виробник": "", "Ціна_за_літр": ""}
        resp.raise_for_status()
    except requests.RequestException:
        return {"Виробник": "", "Ціна_за_літр": ""}

    soup = BeautifulSoup(resp.text, "html.parser")

    brand_tag = soup.select_one("a[href*='/brand/']")
    brand = brand_tag.get_text(strip=True) if brand_tag else ""

    price_l_tag = soup.select_one("div.one_price")
    price_l = price_l_tag.get_text(strip=True) if price_l_tag else ""

    return {"Виробник": brand, "Ціна_за_літр": price_l}


# ── Головна функція ───────────────────────────────────────────
def main() -> pd.DataFrame:
    print("=== Крок 1: завантажуємо каталог ===")
    catalog = load_catalog()
    if catalog.empty:
        print("Каталог порожній — перевірте URL або з'єднання.")
        return catalog

    print(f"\nЗнайдено {len(catalog)} товарів. Починаємо збір деталей…")
    print("=== Крок 2: завантажуємо деталі товарів ===")

    details_list = []
    for i, row in catalog.iterrows():
        print(f"  [{i + 1}/{len(catalog)}] {row['URL']}")
        details = get_details(row["URL"])
        details_list.append(details)
        time.sleep(DELAY)

    details_df = pd.DataFrame(details_list)
    result = pd.concat([catalog.reset_index(drop=True), details_df], axis=1)

    # ── Очищення ──
    result["Ціна"] = (
        result["Ціна"]
        .str.replace("грн.", "", regex=False)
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )
    for col in ["Назва", "Ціна_за_літр", "Виробник"]:
        result[col] = result[col].str.strip()

    result = result[["Назва", "Ціна", "Ціна_за_літр", "Виробник", "URL"]]

    # ── Зберігаємо ──
    out_file = "growex_zzr.xlsx"
    result.to_excel(out_file, index=False)
    print(f"\n✅ Готово! Збережено {len(result)} рядків → {out_file}")
    return result


if __name__ == "__main__":
    df = main()
    print(df.head())
