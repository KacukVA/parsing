import requests
from bs4 import BeautifulSoup
import pymongo
from datetime import datetime


db_client = pymongo.MongoClient('mongodb://localhost:27017/')
db = db_client['mongo_db']


def get_exchange_rates_mono():
    documents = []
    link = 'https://api.monobank.ua/bank/currency'
    response = requests.get(link)
    if response.status_code == 200:
        result = response.json()
        for dictionary in result:
            if 980 in dictionary.values() and (dictionary['rateBuy'] or dictionary['rateSell']):
                currency = dictionary['currencyCodeA']
                buy = float(dictionary['rateBuy'])
                sell = float(dictionary['rateSell'])
                document = {
                    'date': datetime.today(),
                    'source': link,
                    'currency': db.currency.find_one({'NumericCode': currency}),
                    'buy': buy,
                    'sell': sell,
                }
                documents.append(document)
        db.exchange_rates.insert_many(documents)


def get_exchange_rates_privat():
    documents = []
    link = 'https://api.privatbank.ua/p24api/pubinfo?exchange&coursid=5'
    response = requests.get(link)
    if response.status_code == 200:
        result = response.json()
        for dictionary in result:
            if dictionary['buy'] or dictionary['sale']:
                currency = dictionary['ccy']
                buy = float(dictionary['buy'])
                sell = float(dictionary['sale'])
                document = {
                    'date': datetime.today(),
                    'source': link,
                    'currency': db.currency.find_one({'AlphabeticCode': currency}),
                    'buy': buy,
                    'sell': sell,
                }
                documents.append(document)
        db.exchange_rates.insert_many(documents)


def get_exchange_rates_vkurse():
    documents = []
    link = 'https://vkurse.dp.ua/course.json'
    response = requests.get(link)
    if response.status_code == 200:
        result = response.json()
        for key in result:
            if key == 'Dollar':
                currency = db.currency.find_one({'AlphabeticCode': 'USD'}),
            elif key == 'Euro':
                currency = db.currency.find_one({'AlphabeticCode': 'EUR'}),
            elif key == 'Pln':
                currency = db.currency.find_one({'AlphabeticCode': 'PLN'}),
            else:
                break
            buy = float(result[key]['buy'])
            sell = float(result[key]['sale'])
            document = {
                'date': datetime.today(),
                'source': link,
                'currency': currency,
                'buy': buy,
                'sell': sell,
            }
            documents.append(document)
        db.exchange_rates.insert_many(documents)


def get_exchange_rates_sensebank():
    documents = []
    link = 'https://sensebank.com.ua/currency-exchange'
    response = requests.get(link)
    if response.status_code == 200:
        parsed_html = BeautifulSoup(response.content, 'html.parser')
        tabs_items = parsed_html.find(class_='exchange-rate-tabs__items')
        for tab_item in tabs_items:
            title = tab_item.find_all('h3', class_='exchange-rate-tabs__item-label')
            price = tab_item.find_all('h4', class_='exchange-rate-tabs__info-value')
            currency = title[0].text.strip()
            buy = float(price[0].text.strip())
            sell = float(price[1].text.strip())
            document = {
                'date': datetime.today(),
                'source': link,
                'currency':  db.currency.find_one({'AlphabeticCode': currency}),
                'buy': buy,
                'sell': sell,
            }
            documents.append(document)
        db.exchange_rates.insert_many(documents)


def get_exchange_rates_oschadbank():
    documents = []
    link = 'https://www.oschadbank.ua/currency-rate'
    response = requests.get(link)
    if response.status_code == 200:
        parsed_html = BeautifulSoup(response.content, 'html.parser')
        table = parsed_html.find(class_='heading-block-currency-rate__table-body')
        for tr in table:
            row = tr.find_all(class_='heading-block-currency-rate__table-txt')
            currency = row[1].text.strip()
            buy = float(row[3].text.strip())
            sell = float(row[4].text.strip())
            document = {
                'date': datetime.today(),
                'source': link,
                'currency': db.currency.find_one({'AlphabeticCode': currency}),
                'buy': buy,
                'sell': sell,
            }
            documents.append(document)
        db.exchange_rates.insert_many(documents)


def get_currency():
    response = requests.get('https://pkgstore.datahub.io/core/currency-codes/'
                            'codes-all_json/data/029be9faf6547aba93d64384f7444774/codes-all_json.json')
    if response.status_code == 200:
        db.currency.insert_many(list(response.json()))


if __name__ == '__main__':
    list_of_collections = db.list_collection_names()
    if 'currency' not in list_of_collections:
        get_currency()
    get_exchange_rates_mono()
    get_exchange_rates_privat()
    get_exchange_rates_vkurse()
    get_exchange_rates_sensebank()
    get_exchange_rates_oschadbank()
