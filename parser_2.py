from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime

BASE_URL = "https://lnzweb.com"
CATALOG_URL = "https://lnzweb.com/defenda-zzr"

def calc_price_per_liter(price_str, volume_str):
    try:
        price = float(re.sub(r"[^\d.]", "", price_str.replace(",", ".")))
        vol_match = re.search(r"([\d.]+)\s*(л|кг|мл)", volume_str)
        if not vol_match:
            return ""
        vol = float(vol_match.group(1))
        unit = vol_match.group(2)
        if vol == 0:
            return ""
        result = round(price / vol, 2)
        return f"{result} грн/{unit}"
    except:
        return ""

def parse_lnz():
    parse_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    all_products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({"Accept-Language": "uk-UA,uk;q=0.9"})

        print(f"Відкриваю: {CATALOG_URL}")
        page.goto(CATALOG_URL, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        click_count = 0
        while True:
            try:
                clicked = page.evaluate("""
                    () => {
                        const imgs = document.querySelectorAll('img[src*="more-products"]');
                        if (imgs.length > 0) {
                            let el = imgs[0].closest('a') || imgs[0].parentElement;
                            if (el) { el.click(); return true; }
                        }
                        return false;
                    }
                """)
                if not clicked:
                    break
                page.wait_for_load_state("networkidle")
                time.sleep(2)
                click_count += 1
                print(f"Клік {click_count} — завантажуємо більше товарів...")
            except Exception as e:
                print(f"Зупинка: {e}")
                break

        print(f"Всього кліків: {click_count}")
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.product")
    print(f"Знайдено карток: {len(cards)}")

    for card in cards:
        name_el = card.select_one("div.product-name")
        name = name_el.get_text(strip=True) if name_el else ""
        price_el = card.select_one("div.product-price")
        price = price_el.get_text(strip=True) if price_el else ""
        volumes = card.select("div.product-volume")
        volume = volumes[0].get_text(strip=True) if len(volumes) > 0 else ""
        active_substance = volumes[1].get_text(strip=True) if len(volumes) > 1 else ""
        price_per_l = calc_price_per_liter(price, volume)
        brand_el = card.select_one("div.product-culture")
        brand = brand_el.get_text(strip=True) if brand_el else ""
        link_el = card.select_one("a[href]")
        href = link_el["href"] if link_el else ""
        url = href if href.startswith("http") else BASE_URL + href

        if not name:
            continue

        all_products.append({
            "Назва": name,
            "Ціна": price,
            "Ціна за літр": price_per_l,
            "Обсяг": volume,
            "Діюча речовина": active_substance,
            "Бренд": brand,
            "URL": url
        })

    df = pd.DataFrame(all_products)
    df["Дата_парсінгу"] = parse_date  # ← додано
    df.to_excel("lnz_defenda.xlsx", index=False)
    print(f"\n✅ Збережено {len(df)} товарів → lnz_defenda.xlsx")
    return df

if __name__ == "__main__":
    df = parse_lnz()
    print(df.head(10))
