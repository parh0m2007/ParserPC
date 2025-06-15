from __future__ import print_function

import logging
import os
from selenium_stealth import stealth
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
from datetime import datetime, timedelta
import os.path
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from GoogleSheets import GoogleSheet

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database path and other configurations
DB_PATH = 'my_database.db'
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, как у Gecko) /17.5 Safari/605.1.15'
SCRAPING_URLS = {
    'videocards': 'https://market.yandex.ru/catalog--videokarty/26912670/list?suggest_text=видеокарта&hid=91031&rs=eJxFy71qAjEAAGADZ0kjlIyHpZLxtktyJpdNcHQqfYEQ80MP0mqNt4iLqy8gQkcH38TnEboKFRGcPz7-8g5WGdyduhewjr3ReAvQK4IQYJIDAvBT_9n5YNq41Ix0ig4a3rCXZyTDuD-wJrUmamsWszb5qCvdLP1X0smbhf0kv0dUnPcBVbcFc0TQdb1NTWrsI82jab7vRXNyOJJi8_eDwQQGqpSYOvXBWc05rQWrqRKqlE4aqwSTXFgehjRYL51wvqqZp5LSkpXsHwHPOns%2C&rt=9',
    'processors': 'https://market.yandex.ru/catalog--protsessory-cpu/26912730/list?text=процессор&hid=91019&rs=eJx90DFLw0AYxvFeiRJOqCl0CBH13LIldzGXZhIcncQvcJzXiwmkaeglCCpIZ0FwcHZxdRCEOrvo4iD4AXRx9RMIhhhsp-7_H-_DSzr74ETTL56XfsBjB-zsvrfhFtR1YCDTQpaxbK0OZMTLtGAey_mRJKhlt_6TLurOJ3guwXUCTICA0bY2Rc6Zikc5SzJRFuw4KWIm8iFThcxEktZkuyYrpoY0w7A2BFclT5ng41GpZFoNSAo5VExJPhYxeng6tz8_zmDQqB7qVQotVoygmym0764vQXNONyGCFVw_5CoRM5enPMlm6uuW22_TKwBJo_5Gri1Q6PX71L5_mVRf2NM9GVFKI--A4IAQ0g8Dz_VD6ojIoz4RvhtIHJFQun0ehcHAoyGWLnVdBzv4F-W8dT0%2C&rt=9&qrfrom=4&resale_goods=resale_new',
    'motherboards': 'https://market.yandex.ru/catalog--materinskie-platy/26912770/list?suggest_text=материнская%20плата&hid=91020&rs=eJx9krFLAlEcgD0wuV5QCg5yUvwEh8PFe-_uPHUJw8kpw8nleHc-S7A0T5cgqqEgqDa3oKXBpSGI2oPoLwj6O4JGITsVfbxyvu_7fb9775HVbekoKF-9Lw2lty-0ufURQgkky1IYYlGIhkPKWo3Vaa_ZtYndpruMQEANoPgEQYBGyPIU0f2PU78M5Xkf-36GQwpQEBGTS0ggzScw5cychKa7Ib1GEooiYHJKC1Dyi_zGlBCUR4acYYIghg0MwYPF3eaQKVTHEH1oFKmKIRyIQEUOEQ_KQF0P8LgooIsJPSUBCDI2PDvvIin-BYSXuUq9Hm7ZLO62ex5qjaY0u2_fsbqsNtwOkXl60kDVRfp9VWIH_FI_RjrtnE7h7QepD_0ZCui_KsSAER-K6Q72GO_PaTdo4mFn3A1Afh4fInNTG0sbiGjx_Hqtn_XMJEa4VX9CCp9cT9fr7dPSAS3KOZp1MLUd3CLYIIUZGs7KWZqZdy8C6kzMZNoxaNuswnTLHpTmzjpmW0bQ0TuMf6FPe5Q%2C%2C&rt=9&qrfrom=4&resale_goods=resale_new',
    'ram': 'https://market.yandex.ru/search?text=озу&hid=191211&rs=eJwzUg1grGLheH2V-xMjBweDBIMCkPmXcd6ERXa9THf4ntpNZVpw4JKdUySXNBcHB6OAggSLAosAmxRnSmpaYmlOSbyRAoMGA1ySS4ELWdIYRZJRgRFZ0hAkKcDoxZGWYmaYmpZkEmRkaG5kaWhqYmBuYmpqqm9kamBibGBubmicaAZEhmmmxsZmBgapiRZGJgZAhr6hviEAqWcrpw%2C%2C&rt=9&resale_goods=resale_new&qrfrom=4',
    'ssd': 'https://market.yandex.ru/search?text=ssd&hid=16309373&hid=16309374&rs=eJwzsgpgrGLl-LvjMfsnRhkOBoGFh1glGBRAfIV_QEIDxNIAsf4yLrt41q6X6XYbo_1UpgVSv-2cjjFxGXJxcDAKMEowKjAKMEnJJxckxhdn5BfEZ-Yll5bEl2eWZMQnF-TGF5ek5iVn5igwaDBw6YO1cIC1CEhJJCUWZybHJycW5ZcWp-bEF-QkZubFl-QXKLxZxKXR-HcVI5cpWIOyBIsCC1CDXHJicWliDkKHYXxmSWpucXxxamJRcobCozdxGmv7dzBymUO1cSlwAbUp4NcWbwTW-LEPrpEbl0ZjbBqX_pkHcyg3LocaYzr07-95wNDz4jBOMTM0TUwyDzIyNDeyNDQ1MTUwtzQy1TdKNDdOtjQ3MTNJSUqxTDNNsUi1MDYxski0MDIxMDMw0DfUNwQAVVSPpQ%2C%2C',
    'hdd': 'https://market.yandex.ru/search?clid=703&text=купить%20жесткий%20диск%20в%20перми&rs=eJxVjbsOgjAUQC0xBrsICYPBwbq50XsRiqujk_EHGrhSIUEkAeOv-jk-okmdzzk56BzY7sH4grsu88R8LMbeJJyeSpPfmkGjGK1HfPWFoQhfcPaDse7yc_mv-MK3FbCU34ILbi_ivz4Qgd2j1cNHYXMmmOeES-py3VfXTtct3QZ9r4dKU3fR_VC2VDfvxHP27jaTWGS5OiIoTFBCkijMVES0STenlJShEiAuCmmkIdpKgyBTKSOI4AkjCj2c',
    'cooler': 'https://market.yandex.ru/catalog--kulery-i-sistemy-okhlazhdeniia-dlia-kompiuterov/26912910/list?text=%D0%BA%D1%83%D0%BB%D0%B5%D1%80&hid=818965&rs=eJwz8lCy5uK6sOti84XdF7ZebBC4daaXS4mFg0GAFUwyQEgNhiyGKg5jEwMzC1NjgwbGqf8MuxiZOBgDGKtYOICcDYwMTt1MXIZcHByMAowSjAqMAkxS8skFifHFGfkF8Zl5yaUl8eWZJRnxyQW58cUlqXnJmTkKQGO59MBaOMBaBKQkkhKLM5PjkxOL8kuLU3PiC3ISM_PiS_ILFB5_5NRYeDqbywysnluCS4ELqF4hObG4NDEHocE4PrMkNbc4vjg1sSg5I95I4dAjTo2Nc924TMH6VCRYFFiA-uTQ9Rmh6FP4vClVY9v8iYxQbdwSsgoc2LShWgd25NqTDcAg8OJISU0ySTFLSwsyMjQ3MjI3NDG3NDcwMdQ3N7EwMDZLNEtOTrY0SDMwSzJKNbcwS05KTjZMNTAzMNA31DcEAMczdew%2C&rt=11&glfilter=21194330%3A34068530',
    'psu': 'https://market.yandex.ru/catalog--bloki-pitaniia-dlia-kompiuterov/26912850/list?text=%D0%B1%D0%BB%D0%BE%D0%BA%20%D0%BF%D0%B8%D1%82%D0%B0%D0%BD%D0%B8%D1%8F%20%D0%B4%D0%BB%D1%8F%20%D0%BF%D0%BA&hid=857707&rs=eJx1zk1Kw0AYgOFOCBJHxCl0EdKFn7sshGR-nNSty67ECwyT6cQE0iZ0JgieQFeeoJ7AK_QaXkIPYRGRLHT_vPCy6BY9htHHm7h5DvAFjiJEIE4gIUfJ2cpWemi94qrX95bBJJ38kilMx4T-QWYwGxM2IvSboDiEkATJuem1cnXXq2ZjBq8eGl8r06-V83ZjmnaUnMQIECHJ3Gg36FYZve0GZ9vDY-Pt2inf9fDyepruPjMsfxIM-JDAf4mzemtqxWD3fpzuny5JsIyqxcIKXfA7RgvGCiqvuOQiz0rOrismqDDSGlHJsiwqzjg3K2pzmecZzegXjslWXQ%2C%2C&rt=9'

}

# Ensure the database directory exists
db_dir = os.path.dirname(DB_PATH)
if db_dir and not os.path.exists(db_dir):
    os.makedirs(db_dir)

# Register adapters and converters for datetime
def adapt_datetime(ts):
    return ts.strftime('%Y-%m-%d %H:%M:%S')

def convert_datetime(s):
    return datetime.strptime(s.decode('utf-8'), '%Y-%m-%d %H:%M:%S')

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

# Database initialization function
def init_db():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()

        # Create tables if they don't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS графический_процессор (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                имя TEXT NOT NULL,
                категория INTEGER NOT NULL,
                дата TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                brand TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS процессор (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                имя TEXT NOT NULL,
                категория INTEGER NOT NULL,
                дата TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                brand TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS материнская_плата (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                имя TEXT NOT NULL,
                категория INTEGER NOT NULL,
                сокет TEXT NOT NULL,
                дата TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                brand TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS озу (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                имя TEXT NOT NULL,
                категория INTEGER NOT NULL,
                DDR TEXT NOT NULL,
                MHz INTEGER NOT NULL,
                container INTEGER NOT NULL,
                дата TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ssd (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category INTEGER NOT NULL,
                container TEXT NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hdd (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category INTEGER NOT NULL,
                container TEXT NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cooler (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                socket TEXT NOT NULL,
                category INTEGER NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS block_pitanie (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category INTEGER NOT NULL,
                Vt INTEGER NOT NULL,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
                    CREATE TABLE IF NOT EXISTS cpu (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        category INTEGER NOT NULL,
                        socket TEXT NOT NULL,
                        amd_or_intel INTEGER NOT NULL,
                        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

        conn.commit()
        return conn, cursor
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return None, None

# Function to insert data into the database
def insert_into_db(cursor, table, *data):
    try:
        placeholders = ', '.join('?' * len(data))
        cursor.execute(f'''
            INSERT INTO {table} VALUES (NULL, {placeholders}, CURRENT_TIMESTAMP)
        ''', data)
    except sqlite3.Error as e:
        logging.error(f"Error inserting into database: {e}")

# WebDriver initialization function
options = webdriver.ChromeOptions()
options.add_argument(f"user-agent={USER_AGENT}")

def init_webdriver():
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        stealth(driver,
                vendor="Google Inc",
                platform="Win32",
                renderer="Intel Iris OpenGL Engine")
        return driver
    except Exception as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return None

# Determine the brand from the product name
def determine_brand(name_text, brand_keywords):
    name_text_lower = name_text.lower()
    for brand, keywords in brand_keywords.items():
        if any(keyword in name_text_lower for keyword in keywords):
            return brand
    return 'Other'

# Scrape data function for graphics cards and processors
def scrape_data(driver, url, category_thresholds, brand_keywords, table_name):
    try:
        driver.get(url)
        sleep(10)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
        prices = soup.find_all('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})

        scraped_data = []
        for name, price in zip(contents, prices):
            try:
                name_text = name.get_text()
                price_text = price.get_text().replace('руб', '').replace(' ', '').replace('\u2009', '')
                price_value = int(price_text)

                if price_value < category_thresholds[0]:
                    category = 1
                elif category_thresholds[0] <= price_value < category_thresholds[1]:
                    category = 2
                elif category_thresholds[1] <= price_value < category_thresholds[2]:
                    category = 3
                else:
                    category = 4

                brand = determine_brand(name_text, brand_keywords)

                gs = GoogleSheet()
                test_range = 'Видеокарта!A1:C1000'
                test_values = [
                    [name_text, price_text,brand]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((name_text, category, brand))
                logging.info(f"{name_text} - {price_text} руб - Категория {category} - Бренд {brand}")
            except ValueError as e:
                logging.error(f"Value error: {e}")
            except Exception as e:
                logging.error(f"Error processing data: {e}")
        return scraped_data


    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Scrape data function for motherboards
def scrape_motherboards(driver, url):
    try:
        driver.get(url)
        sleep(10)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("span", {"class":'ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp'})
        prices = soup.find_all('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
        product_containers = soup.find_all('div', class_='')

        scraped_data = []
        for name, price, product in zip(contents, prices, product_containers):
            try:
                name_text = name.get_text()
                price_text = price.get_text().replace('руб', '').replace(' ', '').replace('\u2009', '')
                price_value = int(price_text)

                # Extract the socket information
                socket_text = 'Unknown'
                divs = product.find_all('div', {'class': '_2Ce4O'})
                for div in divs:
                    div_text = div.get_text().strip().lower()
                    if 'сокет:' in div_text:
                        socket_text = div_text.replace("сокет", "").replace(":", "").strip().upper()
                        break

                if socket_text == 'Unknown':
                    # Fallback: Check for common socket types in the name_text
                    common_sockets = ['AM4', 'AM5', 'LGA1200', 'LGA1700', 'LGA1150', 'LGA1151v2']
                    for socket in common_sockets:
                        if socket.lower() in name_text.lower():
                            socket_text = socket
                            break

                if socket_text == 'Unknown':
                    logging.info(f"Skipping {name_text} - no socket found")
                    continue

                if price_value < 10000:
                    category = 1
                elif 10000 <= price_value < 25000:
                    category = 2
                elif 25000 <= price_value < 50000:
                    category = 3
                else:
                    category = 4

                if socket_text in ['AM4', 'AM5']:
                    brand = 'AMD'
                elif socket_text in ['LGA1200', 'LGA1700', 'LGA1150', 'LGA1151v2']:
                    brand = 'Intel'
                else:
                    brand = 'Other'

                gs = GoogleSheet()
                test_range = 'Материнская плата!A1:C1000'
                test_values = [
                    [name_text, price_text, brand]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((name_text, category, socket_text, brand))
                logging.info(
                    f"{name_text} - {price_text} руб - Категория {category} - Сокет {socket_text} - Бренд {brand}")
            except ValueError as e:
                logging.error(f"Value error: {e}")
            except Exception as e:
                logging.error(f"Error processing data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Scrape data function for RAM
def scrape_ram(driver, url):
    try:
        driver.get(url)
        sleep(10)  # Wait for the page to load completely

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("div", {"class": "_1H-VK"})  # Product card container

        scraped_data = []
        for content in contents:
            try:
                # Extract product title
                title_tag = content.find("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
                title = title_tag.get_text(strip=True) if title_tag else "Unknown"

                # Extract product price
                price_tag = content.find('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
                price_text = price_tag.get_text(strip=True).replace('руб', '').replace(' ', '').replace('\u2009', '') if price_tag else "0"
                try:
                    price_value = int(price_text)
                except ValueError:
                    logging.error(f"Invalid price format for {title}: {price_text}")
                    continue

                # Extract additional details like DDR type, frequency, container, etc.
                ddr_type = 'Unknown'
                frequency = 'Unknown'
                container = 'Unknown'

                divs = content.find_all('div', {'class': '_2Ce4O'})
                for div in divs:
                    div_text = div.get_text().strip().lower()
                    if 'тип:' in div_text:
                        ddr_type = div_text.replace("тип:", "").strip().upper()
                        if "DIMM 288-PIN" in ddr_type or "DIMM 240-PIN" in ddr_type :
                            ddr_type = ddr_type.replace("DIMM 288-PIN", "").replace("DIMM 240-PIN", "").strip()
                    elif 'тактовая частота:' in div_text:
                        frequency = div_text.replace("тактовая частота:", "").replace("мгц", "").strip()
                    elif 'количество модулей в комплекте:' in div_text:
                        container = div_text.replace("количество модулей в комплекте:", "").replace("шт.", "").strip()

                if ddr_type == 'Unknown' or frequency == 'Unknown' or container == 'Unknown':
                    logging.info(f"Skipping {title} - incomplete details")
                    continue

                # Determine category based on price
                category = 4
                if price_value < 5000:
                    category = 1
                elif 5000 <= price_value < 10000:
                    category = 2
                elif 10000 <= price_value < 15000:
                    category = 3

                gs = GoogleSheet()
                test_range = 'Оперативная память!A1:C1000'
                test_values = [
                    [title, price_text, ddr_type,frequency,container]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((title, category, ddr_type, int(frequency), int(container)))
                logging.info(f"{title} - {price_text} руб - Категория {category} - {ddr_type} - {frequency} MHz - {container} шт.")
            except Exception as e:
                logging.error(f"Error processing product data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Scrape data function for SSDs
def scrape_ssd(driver, url):
    try:
        driver.get(url)
        sleep(10)  # Wait for the page to load completely

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("div", {"class": "_1H-VK"})  # Product card container

        scraped_data = []
        for content in contents:
            try:
                # Extract product title
                title_tag = content.find("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
                title = title_tag.get_text(strip=True) if title_tag else "Unknown"

                # Extract product price
                price_tag = content.find('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
                price_text = price_tag.get_text(strip=True).replace('руб', '').replace(' ', '').replace('\u2009', '') if price_tag else "0"
                try:
                    price_value = int(price_text)
                except ValueError:
                    logging.error(f"Invalid price format for {title}: {price_text}")
                    continue

                # Extract container size
                container = 'Unknown'
                divs = content.find_all('div', {'class': '_2Ce4O'})

                for div in divs:
                    div_text = div.get_text().strip().lower()
                    if 'емкость:' in div_text:
                        container = div_text.replace("емкость: ", "").replace(" гб", "ГБ").strip().upper()

                if container == 'Unknown':
                    logging.info(f"Skipping {title} - incomplete details")
                    continue

                # Determine category based on price
                category = 4
                if price_value < 5000:
                    category = 1
                elif 5000 <= price_value < 10000:
                    category = 2
                elif 10000 <= price_value < 15000:
                    category = 3

                gs = GoogleSheet()
                test_range = 'SSD!A1:C1000'
                test_values = [
                    [title, price_text, container]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((title, category, container))
                logging.info(f"{title} - {price_text} руб - Категория {category} - {container}")
            except Exception as e:
                logging.error(f"Error processing product data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []



def scrape_hdd(driver, url):
    try:
        driver.get(url)
        sleep(10)  # Wait for the page to load completely

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("div", {"class": "_1H-VK"})  # Product card container

        scraped_data = []
        for content in contents:
            try:
                # Extract product title
                title_tag = content.find("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
                title = title_tag.get_text(strip=True) if title_tag else "Unknown"

                # Extract product price
                price_tag = content.find('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
                price_text = price_tag.get_text(strip=True).replace('руб', '').replace(' ', '').replace('\u2009', '') if price_tag else "0"
                try:
                    price_value = int(price_text)
                except ValueError:
                    logging.error(f"Invalid price format for {title}: {price_text}")
                    continue

                # Extract container size
                container = 'Unknown'
                divs = content.find_all('div', {'class': '_2Ce4O'})

                for div in divs:
                    div_text = div.get_text().strip().lower()
                    if 'емкость:' in div_text:
                        container = div_text.replace("емкость: ", "").replace(" гб", "ГБ").strip().upper()

                if container == 'Unknown':
                    logging.info(f"Skipping {title} - incomplete details")
                    continue

                # Determine category based on price
                category = 4
                if price_value < 5000:
                    category = 1
                elif 5000 <= price_value < 10000:
                    category = 2
                elif 10000 <= price_value < 15000:
                    category = 3

                gs = GoogleSheet()
                test_range = 'HDD!A1:C1000'
                test_values = [
                    [title, price_text, container]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((title, category, container))
                logging.info(f"{title} - {price_text} руб - Категория {category} - {container}")
            except Exception as e:
                logging.error(f"Error processing product data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Scrape data function for coolers
def scrape_cooler(driver, url):
    try:
        driver.get(url)
        sleep(10)  # Wait for the page to load completely

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("div", {"class": "_1H-VK"})  # Product card container

        scraped_data = []
        for content in contents:
            try:
                # Extract product title
                title_tag = content.find("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
                title = title_tag.get_text(strip=True) if title_tag else "Unknown"

                # Extract product price
                price_tag = content.find('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
                price_text = price_tag.get_text(strip=True).replace('руб', '').replace(' ', '').replace('\u2009', '') if price_tag else "0"
                try:
                    price_value = int(price_text)
                except ValueError:
                    logging.error(f"Invalid price format for {title}: {price_text}")
                    continue

                # Extract socket compatibility
                socket = 'ANY'
                divs = content.find_all('div', {'class': '_2Ce4O'})
                for div in divs:
                    div_text = div.get_text().strip().lower()
                    if 'сокет:' in div_text:
                        sockets = div_text.replace("сокет:", "").strip().upper().split(',')
                        socket = 'ANY' if len(sockets) > 1 else sockets[0]

                # Determine category based on price
                category = 3
                if price_value < 5000:
                    category = 1
                elif 5000 <= price_value < 10000:
                    category = 2

                gs = GoogleSheet()
                test_range = 'Кулер!A1:C1000'
                test_values = [
                    [title, price_text, socket]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((title, socket, category))
                logging.info(f"{title} - {price_text} руб - Категория {category} - Сокет {socket}")
            except Exception as e:
                logging.error(f"Error processing product data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Scrape data function for power supplies
def scrape_psu(driver, url):
    try:
        driver.get(url)
        sleep(10)  # Wait for the page to load completely

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("div", {"class": "_1H-VK"})  # Product card container

        scraped_data = []
        for content in contents:
            try:
                # Extract product title
                title_tag = content.find("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
                title = title_tag.get_text(strip=True) if title_tag else "Unknown"

                # Extract product price
                price_tag = content.find('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
                price_text = price_tag.get_text(strip=True).replace('руб', '').replace(' ', '').replace('\u2009', '') if price_tag else "0"
                try:
                    price_value = int(price_text)
                except ValueError:
                    logging.error(f"Invalid price format for {title}: {price_text}")
                    continue

                # Extract power (Vt)
                power = 'Unknown'
                divs = content.find_all('div', {'class': '_2Ce4O'})
                for div in divs:
                    div_text = div.get_text().strip().lower()
                    if 'мощность:' in div_text:
                        power = div_text.replace("мощность:", "").replace("вт", "").strip()

                if power == 'Unknown':
                    logging.info(f"Skipping {title} - incomplete details")
                    continue

                # Determine category based on price
                category = 3
                if price_value < 5000:
                    category = 1
                elif 5000 <= price_value < 10000:
                    category = 2

                gs = GoogleSheet()
                test_range = 'Блок питания!A1:C1000'
                test_values = [
                    [title, price_text, power]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((title, category, int(power)))
                logging.info(f"{title} - {price_text} руб - Категория {category} - {power} Вт")
            except Exception as e:
                logging.error(f"Error processing product data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Scrape data function for CPUs
def scrape_cpus(driver, url):
    try:
        driver.get(url)
        sleep(10)  # Wait for the page to load completely

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        contents = soup.find_all("div", {"class": "_1H-VK"})  # Product card container

        scraped_data = []
        for content in contents:
            try:
                # Extract product title
                title_tag = content.find("span", {"class": "ds-text ds-text_lineClamp_2 ds-text_weight_med ds-text_color_text-primary ds-text_typography_lead-text ds-text_lead-text_normal ds-text_lead-text_med ds-text_lineClamp"})
                title = title_tag.get_text(strip=True) if title_tag else "Unknown"

                # Extract product price
                price_tag = content.find('span', {'class': 'ds-text ds-text_weight_bold ds-text_color_price-term ds-text_typography_headline-5 ds-text_headline-5_tight ds-text_headline-5_bold'})
                price_text = price_tag.get_text(strip=True).replace('руб', '').replace(' ', '').replace('\u2009', '') if price_tag else "0"
                try:
                    price_value = int(price_text)
                except ValueError:
                    logging.error(f"Invalid price format for {title}: {price_text}")
                    continue

                # Extract socket type
                socket = 'Unknown'
                socket_tags = content.find_all('div', {'class': '_2Ce4O'})
                for socket_tag in socket_tags:
                    socket_text = socket_tag.get_text().strip().lower()
                    if 'сокет:' in socket_text:
                        socket = socket_text.replace("сокет:", "").strip().upper()
                        break
                if socket == 'Unknown':
                    logging.info(f"Skipping {title} - incomplete details")
                    continue
                # Determine category based on price
                category = 4
                if price_value < 5000:
                    category = 1
                elif 5000 <= price_value < 10000:
                    category = 2
                elif 10000 <= price_value < 20000:
                    category = 3

                # Determine if CPU is AMD or Intel based on title
                if any(keyword in title.lower() for keyword in ['amd', 'ryzen']):
                    brand = 'AMD'
                elif any(keyword in title.lower() for keyword in ['intel', 'core']):
                    brand = 'Intel'
                else:
                    brand = 'Unknown'

                gs = GoogleSheet()
                test_range = 'CPU!A1:C1000'
                test_values = [
                    [title, price_text, category,socket,brand]
                ]
                gs.appendRow(test_range, test_values)

                scraped_data.append((title, category, socket, brand))
                logging.info(f"{title} - {price_text} руб - Категория {category} - Сокет {socket} - {brand}")
            except Exception as e:
                logging.error(f"Error processing product data: {e}")
        return scraped_data
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return []

# Main function
def main():
    conn, cursor = init_db()
    if conn is None or cursor is None:
        logging.error("Failed to initialize database. Exiting.")
        return

    driver = init_webdriver()
    if driver is None:
        logging.error("Failed to initialize WebDriver. Exiting.")
        return

    try:
        # Scrape and insert data for graphics cards
        scraped_videocards = scrape_data(driver, SCRAPING_URLS['videocards'], [30000, 60000, 100000],
                                         {'NVIDIA': ['geforce', 'rtx', 'gtx'], 'AMD': ['radeon', 'xt', 'rx']},
                                         'графический_процессор')
        for name, category, brand in scraped_videocards:
            insert_into_db(cursor, 'графический_процессор', name, category, brand)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        # Scrape and insert data for CPUs
        scraped_cpus = scrape_cpus(driver, SCRAPING_URLS['processors'])
        for name, category, socket, amd_or_intel in scraped_cpus:
            insert_into_db(cursor, 'cpu', name, category, socket, amd_or_intel)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        # Scrape and insert data for motherboards
        scraped_motherboards = scrape_motherboards(driver, SCRAPING_URLS['motherboards'])
        for name, category, socket, brand in scraped_motherboards:
            insert_into_db(cursor, 'материнская_плата', name, category, socket, brand)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        # Scrape and insert data for RAM
        scraped_ram = scrape_ram(driver, SCRAPING_URLS['ram'])
        for name, category, ddr, mhz, container in scraped_ram:
            insert_into_db(cursor, 'озу', name, category, ddr, mhz, container)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        # Scrape and insert data for SSDs
        scraped_ssd = scrape_ssd(driver, SCRAPING_URLS['ssd'])
        for name, category, container in scraped_ssd:
            insert_into_db(cursor, 'ssd', name, category, container)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        scraped_hdd = scrape_hdd(driver, SCRAPING_URLS['hdd'])
        for name, category, container in scraped_hdd:
            insert_into_db(cursor, 'hdd', name, category, container)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        # Scrape and insert data for coolers
        scraped_cooler = scrape_cooler(driver, SCRAPING_URLS['cooler'])
        for name, socket, category in scraped_cooler:
            insert_into_db(cursor, 'cooler', name, socket, category)

        conn.commit()

        # Short pause between scrapes
        sleep(5)

        # Scrape and insert data for power supplies
        scraped_psu = scrape_psu(driver, SCRAPING_URLS['psu'])
        for name, category, power in scraped_psu:
            insert_into_db(cursor, 'block_pitanie', name, category, power)

        conn.commit()

        logging.info(
            f"Scraped {len(scraped_videocards)} videocards, {len(scraped_cpus)} processors, {len(scraped_motherboards)} motherboards, {len(scraped_ram)} RAM modules, {len(scraped_ssd)} SSDs, {len(scraped_cooler)} coolers, and {len(scraped_psu)} power supplies successfully.")
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
    finally:
        conn.close()
        driver.quit()

if __name__ == '__main__':
    main()
