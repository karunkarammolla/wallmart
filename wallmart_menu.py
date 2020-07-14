# -*- coding: utf-8 -*-
import requests
import pandas as pd
import demjson
import math
import concurrent.futures
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import time
from datetime import datetime
today=datetime.today().strftime('%Y-%m-%d')
class wall_mart_data:

    def __init__(self,urlw,parent,child):
        self.urlw = urlw
        self.parent = parent
        self.child = child
        self.df=None

    def pages(self):
        item_urls = []
        item_id = []

        try:
            url = 'https://grocery.walmart.com/v4/api/products/browse?count=60&offset=0&page=1&storeId=2086&taxonomyNodeId=' + str(self.urlw.split('aisle=')[1])
        except:
            url = 'https://grocery.walmart.com/v4/api/products/browse?count=60&offset=0&page=1&storeId=2086&shelfId='+str(self.urlw.split('?shelfId=')[1])
        gh = requests.get(url).text
        page = demjson.decode(gh)
        page_count = page['totalCount']
        print(self.parent)
        print(self.child)
        print(page_count)
        page = math.ceil(page_count / 60)
        if page == 1:
            response = requests.request("GET", url)
            data = (response.text)
            data = demjson.decode(data)
            ids = data['products']
            for id in ids:
                nom = id['USItemId']
                item_id.append(nom)
        else:
            stf = 0
            for i in range(1, page + 1):
                stf = stf
                try:
                    url = 'https://grocery.walmart.com/v4/api/products/browse?count=60&offset='+str(stf)+'&page='+str(i)+'&storeId=2086&taxonomyNodeId='+str(self.urlw.split('aisle=')[1])
                except:
                    url='https://grocery.walmart.com/v4/api/products/browse?count=60&offset='+str(stf)+'&page='+str(i)+'&storeId=2086&shelfId=' + str(self.urlw.split('?shelfId=')[1])
                stf=stf+60
                print(url)
                response = requests.request("GET", url)
                data = (response.text)
                data = demjson.decode(data)
                ids = data['products']
                for id in ids:
                    nom = id['USItemId']
                    item_id.append(nom)

        print('items_length',len(item_id))
        for k in item_id:
            uyt = 'https://grocery.walmart.com/v3/api/products/' + str(k) + '?itemFields=all&storeId=2086'
            type = self.parent
            child = self.child
            kjh = {'Url':uyt,'Type':type,'Child':child}
            item_urls.append(kjh)
        df=pd.DataFrame(item_urls)
        return df


    def menu(self,url):
        dframe = []
        url = 'https://grocery.walmart.com/tempo?tenant=WM_TO_GO&channel=WWW&pageType=global_header&enrich=iro,athenaunified,tango,karf&p13n=%7B%22userReqInfo%22%3A%7B%22storeIds%22%3A%5B%222086%22%5D%7D%7D&targeting=%7B%22site%22%3A%22gop%22%2C%22storeId%22%3A%222086%22%2C%22deviceType%22%3A%22web%22%2C%22deviceVersion%22%3A%226.0.0%22%2C%22CCMP%22%3A%22true%22%7D&storeId=2086'
        gh = requests.get(url).text
        page = demjson.decode(gh)
        departments = page['modules'][0]['configs']['departments']
        for department in departments[0:]:
            departmento = department['name']['linkText']
            children = department['children']
            for child in children[1:]:
                childo = child['name']['linkText']
                try:
                    url = 'https://grocery.walmart.com/browse/' + childo.replace(' ', '-').replace(',', '-').replace(
                        '--', '-') + '?aisle=' + child['name']['aislePath']
                except:
                    url = 'https://grocery.walmart.com/browse?shelfId=' + child['name']['clickThrough']['value']
                men_data = {'Parent': departmento, 'Child': childo, 'Url': url}
                dframe.append(men_data)
        greta = pd.DataFrame(dframe)
        return greta

def item(record):
    try:
        response = requests.request("GET",record.get('Url'),timeout=20)
    except:
        return [1,record]
    data = demjson.decode(response.text)

    try:
        # name = data.get('basic',{}).get('name')
        name = data['basic']['name']
    except:

        name = ''
    try:

        Price_as_per_weights = data['store']['price']['unit']
    except:

        Price_as_per_weights = ''
    try:

        Price_per_piece = data['store']['price']['displayPrice']
    except:
        Price_per_piece = ''
    try:
        Quantity_unit = data['store']['price']['salesQuantity']
    except:
        Quantity_unit = ''
    try:
        weight = data['store']['price']['priceUnitOfMeasure']
    except:
        weight = ''
    try:
        description = data['detailed']['description']
        description = (BeautifulSoup(description, "html.parser").text).strip()
    except:
        description = ''
    try:
        short_description = data['detailed']['shortDescription']
        short_description = (BeautifulSoup(short_description, "html.parser").text).strip()
    except:
        short_description = ''

    try:
        previous_price = data['store']['price']['previousPrice']
    except:
        previous_price = ''
    type = record.get('Type')
    child = record.get('Child')
    info = {'Name': name, 'Quantity size': Quantity_unit, 'Price per piece': Price_per_piece,
            'Price as per weight': Price_as_per_weights, 'Weight': weight,
            'Details': description + short_description, "Previous price": previous_price, 'Url': record.get('Url'),
            'Type': type,'Scraped_date':today,'Child':child}

    return [0,info]
if __name__ == '__main__':
    gnam = wall_mart_data('pass','fail','worry')
    greta=gnam.menu('https://grocery.walmart.com/tempo?tenant=WM_TO_GO&channel=WWW&pageType=global_header&enrich=iro,athenaunified,tango,karf&p13n=%7B%22userReqInfo%22%3A%7B%22storeIds%22%3A%5B%222086%22%5D%7D%7D&targeting=%7B%22site%22%3A%22gop%22%2C%22storeId%22%3A%222086%22%2C%22deviceType%22%3A%22web%22%2C%22deviceVersion%22%3A%226.0.0%22%2C%22CCMP%22%3A%22true%22%7D&storeId=2086')
    greta = greta[greta['Parent'].isin(['Fruits & Vegetables','Alcohol', 'Deli','Frozen','Meat & Seafood', 'Eggs & Dairy', 'Health & Nutrition', 'Bread & Bakery', 'Beverages','Organic Shop', 'Snacks & Candy', 'Pantry'])]
    # greta = greta[greta['Parent'].isin(['Fruits & Vegetables'])]

    # greta.to_csv(r'C:\Desktop\ankiacrape\output\greta.csv')
    final_urls=[]

    for i in range(len(greta)):
        if i%50==0:
            print(i)
        v = wall_mart_data(greta.iloc[i]['Url'],greta.iloc[i]['Parent'],greta.iloc[i]['Child'])
        final_urls.append(v.pages())
    records = pd.concat(final_urls).to_dict(orient='records')
    gftg = pd.concat(final_urls)
    print('number of items', len(records))
    count  =0
    dfs = []
    failed_records = []
    while records:

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(item, record): record for record in records}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    print('extracting')
                    if data[0]==0:
                        dfs.append(data[1])
                    elif data[0]==1:
                        failed_records.append(data[1])


        records = failed_records
        count+=1
        if count == 5:#number of checks
            break
    grand_data = pd.DataFrame(dfs)
    if failed_records:
        failed_data= pd.DataFrame(failed_records)
        failed_data.to_csv('C:\\Desktop\\ankiacrape\\twitter\\output\\wallmart_weekly_faildata\\'+str(today) + '.csv', index=False)
    grand_data.to_csv('C:\\Desktop\\ankiacrape\\twitter\\output\\wallmart_weekly_data\\'+str(today) + '.csv', index=False)
    a = (dict(grand_data.isnull().sum()))
    b = {'Number of records': len(grand_data)}
    c = {'Number of duplicate values': len(grand_data) - len(grand_data.drop_duplicates())}
    d = {'Scraped_date': today}
    e={'Website':'wallmart'}
    f={'Owner':'Ammolla'}
    a.update(b)
    a.update(c)
    a.update(d)
    a.update(e)
    a.update(f)
    gf = pd.DataFrame(a, index=[0])
    gf.to_csv('C:\\Desktop\\ankiacrape\\output\\' + str(today) + '.csv', index=False)
    connection_string = f"{'root'}:{'Ankitha@143'}@localhost:3306/{'wallmart'}?charset=utf8"
    engine = create_engine(f'mysql://{connection_string}')

    # engine = create_engine("mysql://root:Ankitha@143@localhost/wallmart", echo=True)
    grand_data.to_sql('wall_mart_weekly_data', engine, if_exists='append', index=False)
