# pip install playwright beautifulsoup4
# python -m playwright install chromium

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json, time, csv

BASE_URL = "https://growex.market"
CATEGORY_URL = f"{BASE_URL}/products/zasobi-zahistu-roslin-zzr"

def parse_product_card(card):
    """Парсить одну картку продукту зі списку."""
    product = {}

    # Назва продукту
    name_el = card.select_one("h2, h3, [class*='product_name'], [class*='title']")
    product["name"] = name_el.get_text(strip=True) if name_el else ""

    # Ціна за одиницю (наприклад: 345 грн./кг)
    one_price_el = card.select_one(".one_price")
    product["unit_price"] = one_price_el.get_text(strip=True) if one_price_el else ""

    # Повна ціна (item_price)
    item_price_el = card.select_one("#item_price, .item_price")
    currency_el   = card.select_one(".current_name")
    if item_price_el:
        price_val = item_price_el.get_text(strip=True)
        currency  = currency_el.get_text(strip=True) if currency_el else "грн."
        product["total_price"] = f"{price_val} {currency}"
    else:
        product["total_price"] = ""

    # Наявність
    status_ok = card.select_one(".status_ok")
    status_no = card.select_one(".status_no")
    if status_ok:
        product["availability"] = status_ok.get_text(strip=True)
    elif status_no:
        product["availability"] = status_no.get_text(strip=True)
    else:
        product["availability"] = ""

    # Посилання на продукт
    link_el = card.select_one("a[href]")
    if link_el:
        href = link_el["href"]
        product["url"] = href if href.startswith("http") else BASE_URL + href
    else:
        product["url"] = ""

    # Зображення
    img_el = card.select_one("img[src]")
    if img_el:
        src = img_el.get("src", "")
        product["image"] = src if src.startswith("http") else BASE_URL + src
    else:
        product["image"] = ""

    return product


def parse_growex(headless=True):
    all_products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        page.set_extra_http_headers({
            "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8"
        })

        print(f"Відкриваю: {CATEGORY_URL}")
        page.goto(CATEGORY_URL, wait_until="networkidle", timeout=30000)

        # Скролимо вниз для lazy-load
        for _ in range(5):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.8)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        # Selectors з реального HTML
        cards = (
            soup.select(".product-option")
            or soup.select("[class*='product_item']")
            or soup.select("article")
        )
        print(f"Знайдено карток: {len(cards)}")

        for card in cards:
            product = parse_product_card(card)
            if product.get("name") or product.get("total_price"):
                all_products.append(product)

        # --- Пагінація ---
        page_num = 1
        while True:
            next_btn = page.query_selector("a[rel='next'], .pagination .next, a:has-text('Наступна')")
            if not next_btn:
                break
            page_num += 1
            print(f"Переходжу на сторінку {page_num}...")
            next_btn.click()
            page.wait_for_load_state("networkidle")
            time.sleep(1)

            for _ in range(3):
                page.evaluate("window.scrollBy(0, window.innerHeight)")
                time.sleep(0.6)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            cards = (
                soup.select(".product-option")
                or soup.select("[class*='product_item']")
                or soup.select("article")
            )
            print(f"  Сторінка {page_num}: {len(cards)} карток")
            for card in cards:
                product = parse_product_card(card)
                if product.get("name") or product.get("total_price"):
                    all_products.append(product)

        browser.close()
    return all_products


if __name__ == "__main__":
    products = parse_growex(headless=True)

    print(f"\n{'='*50}")
    print(f"Всього продуктів: {len(products)}")
    print(f"{'='*50}\n")

    for i, p in enumerate(products, 1):
        print(f"{i:3}. {p['name']}")
        print(f"     Ціна/од.: {p['unit_price']}")
        print(f"     Загальна: {p['total_price']}")
        print(f"     Наявність: {p['availability']}")
        print(f"     URL: {p['url']}")

    # Зберегти JSON
    with open("growex_zzr.json", "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    print("\nЗбережено у growex_zzr.json")

    # Зберегти CSV
    with open("growex_zzr.csv", "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name","unit_price","total_price","availability","url","image"])
        writer.writeheader()
        writer.writerows(products)
    print("Збережено у growex_zzr.csv")