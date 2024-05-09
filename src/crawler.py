from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd


class Crawler:
    def __init__(self, url):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        self.driver = webdriver.Chrome(service=Service(
            ChromeDriverManager().install()), options=chrome_options)
        self.driver.get(url)

    def extract(self):
        ls_product = self.driver.find_element(By.CLASS_NAME, "listproduct")
        ls_item = ls_product.find_elements(By.TAG_NAME, "li")
        data = []
        for i in ls_item:
            detail = i.find_element(By.TAG_NAME, 'a')
            tag = detail.find_element(By.CLASS_NAME, 'item-compare').text
            star = len(detail.find_elements(By.CLASS_NAME, 'icon-star'))
            half = 0.5 if len(detail.find_elements(
                By.CLASS_NAME, 'icon-star-half')) == 1 else 0

            name = detail.get_attribute('data-name')
            actual_price = i.get_attribute('data-price')
            brand = detail.get_attribute('data-brand')
            price = detail.get_attribute('data-price')
            id = detail.get_attribute('data-id')

            premium = "1" if detail.get_attribute(
                'class') == 'main-contain premium-product' else "0"

            data.append([id, name, actual_price, price,
                        brand, premium, star + half, tag])

        columns = [['ID', 'Name', 'Actual Price', 'Price',
                    'Brand', 'Premium', 'Rating', 'Tags']]
        df = pd.DataFrame(columns=columns, data=data)
        return df
