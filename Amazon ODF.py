import pandas as pd
import pymssql
import numpy as np

db = pymssql.connect(host="192.168.1.19",port= 1433,
                   # user="Angela Qu",password="Mxyzptlk901104",
                   database="DOMO",charset="utf8")
sqlcmd = 'select * from dbo.Inventory_Master'
inventory = pd.read_sql(sqlcmd, db)
inventory.columns
inventory.head(3)


import requests
import xml.etree.cElementTree as et

LINK = 'https://api.mozenda.com/rest?WebServiceKey=FBF481D8-98E8-421B-B892-CCBE63805ADB&Service=Mozenda10&Operation=View.GetItems&ViewID='
HEADER = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36'}

def get_xml(viewId):
    link = LINK + str(viewId)
    response = requests.get(link, headers = HEADER)
    return response.text

def get_header(node) :
    li = []
    for child in node:
        li.append(child.tag)
    return li

def get_data(itemlist, df, cols):
    for item in itemlist:
        list = []
        for child in item:
            list.append(child.text)
        df = df.append(
                pd.Series(list, index=cols),
                ignore_index=True)
    return df


def get_dataframe_from_mazenda(view_id):
    xml = get_xml(view_id)
    parsedXML = et.fromstring(xml)
    itemlist = parsedXML.find('ItemList')
    header = get_header(itemlist[0])
    df = pd.DataFrame(columns=header)
    df = get_data(itemlist, df, header)
    return df


Amazon_ODF_result = get_dataframe_from_mazenda(11612)



inventory_DS = pd.read_excel(r'P:\Dept - Sales\Ecommerce\Sales Reports\Greg Common Files\Inventory\Inventory_Copy.xlsx')
Amazon_profile = pd.read_csv(r'P:\Dept - Sales\Ecommerce\Sales Reports\Greg Common Files\400 PROFILES.csv')
ODF_Sku = pd.read_excel(r'P:\Dept - Sales\Ecommerce\Sales Reports\Greg Common Files\Master Skus\Fulfillment Master Sku List.xlsx')

sqlcmd_1 = 'SELECT dbo.Inventory_Master.PartNo, dbo.Inventory_Master.Brand, dbo.Inventory_Master.Dsc, Sum(dbo.Inventory_Master.QAV) AS SumOfQAV, Sum(dbo.Inventory_Master.QOH) AS SumOfQOH, Sum(dbo.Inventory_Master.QBO) AS SumOfQBO, Sum(dbo.Inventory_Master.QOO) AS SumOfQOO, Avg(dbo.Inventory_Master.Cost) AS AvgOfCost, dbo.Inventory_Master.[Col 1], dbo.Inventory_Master.[Col 2], dbo.Inventory_Master.[Col 3], dbo.Inventory_Master.[Col 4], dbo.Inventory_Master.[Col 5], dbo.Inventory_Master.[Col 6], dbo.Inventory_Master.[Col 7], dbo.Inventory_Master.[Col 8], dbo.Inventory_Master.[Col 9], dbo.Inventory_Master.[Col 10], dbo.Inventory_Master.MSRP, dbo.Inventory_Master.MAP, dbo.Inventory_Master.Weight, dbo.Inventory_Master.Description, dbo.Inventory_Master.MstrPk, dbo.Inventory_Master.Business, dbo.Inventory_Master.[Category 1], dbo.Inventory_Master.[Category 2], dbo.Inventory_Master.UPC, dbo.Inventory_Master.[Ship-W], dbo.Inventory_Master.[Ship-L], dbo.Inventory_Master.[Ship-D], dbo.Inventory_Master.[Prod-W], dbo.Inventory_Master.[Prod-L], dbo.Inventory_Master.[Prod-D], dbo.Inventory_Master.[Country of Origin], dbo.Inventory_Master.Warranty, dbo.Inventory_Master.UM, dbo.Inventory_Master.Overbox, dbo.Inventory_Master.[UPS Shippable], dbo.Inventory_Master.[ALT P/N], dbo.Inventory_Master.[Box Type], dbo.Inventory_Master.Color, Sum(dbo.Inventory_Master.[Qty 0-90 Days]) AS [SumOfQty 0-90 Days], Sum(dbo.Inventory_Master.[Qty 91-180 Days]) AS [SumOfQty 91-180 Days], Sum(dbo.Inventory_Master.[Last 7]) AS [SumOfLast 7], Sum(dbo.Inventory_Master.[L 30]) AS [SumOfL 30], Sum(dbo.Inventory_Master.[L 60]) AS [SumOfL 60], Sum(dbo.Inventory_Master.[L 90]) AS [SumOfL 90], Sum([L 30] + [L 60]) AS [Total Last 60 Days], Sum([L 30] + [L 60] + [L 90]) AS [Total Last 90 Days], dbo.Inventory_Master.CATA, dbo.Inventory_Master.CATB, dbo.Inventory_Master.CATC, IIf([Ship-W] * [Ship-L] * [Ship-D] = 0, 0, ([Ship-W] * [Ship-L] * [Ship-D]) / 166) AS DimWT FROM dbo.Inventory_Master GROUP BY dbo.Inventory_Master.PartNo, dbo.Inventory_Master.Brand, dbo.Inventory_Master.Dsc, dbo.Inventory_Master.[Col 1], dbo.Inventory_Master.[Col 2], dbo.Inventory_Master.[Col 3], dbo.Inventory_Master.[Col 4], dbo.Inventory_Master.[Col 5], dbo.Inventory_Master.[Col 6], dbo.Inventory_Master.[Col 7], dbo.Inventory_Master.[Col 8], dbo.Inventory_Master.[Col 9], dbo.Inventory_Master.[Col 10], dbo.Inventory_Master.MSRP, dbo.Inventory_Master.MAP, dbo.Inventory_Master.Weight, dbo.Inventory_Master.Description, dbo.Inventory_Master.MstrPk, dbo.Inventory_Master.Business, dbo.Inventory_Master.[Category 1], dbo.Inventory_Master.[Category 2], dbo.Inventory_Master.UPC, dbo.Inventory_Master.[Ship-W], dbo.Inventory_Master.[Ship-L], dbo.Inventory_Master.[Ship-D], dbo.Inventory_Master.[Prod-W], dbo.Inventory_Master.[Prod-L], dbo.Inventory_Master.[Prod-D], dbo.Inventory_Master.[Country of Origin], dbo.Inventory_Master.Warranty, dbo.Inventory_Master.UM, dbo.Inventory_Master.Overbox, dbo.Inventory_Master.[UPS Shippable], dbo.Inventory_Master.[ALT P/N], dbo.Inventory_Master.[Box Type], dbo.Inventory_Master.Color, dbo.Inventory_Master.CATA, dbo.Inventory_Master.CATB, dbo.Inventory_Master.CATC, IIf([Ship-W] * [Ship-L] * [Ship-D] = 0, 0, ([Ship-W] * [Ship-L] * [Ship-D]) / 166) HAVING (((dbo.Inventory_Master.Brand) <> \'%misc%\') AND ((dbo.Inventory_Master.[Category 1]) <> \'%misc%\')) ORDER BY dbo.Inventory_Master.Brand'
inventory_detail_summed = pd.read_sql(sqlcmd_1, db)



ODF_Sku = ODF_Sku[ODF_Sku['Fulfillment_CAT'] == 'Outdoor Furniture']
ODF_Sku = ODF_Sku[['SKU', 'MFG', 'New', 'ASIN']]
ODF_Sku.to_csv(r'P:\Dept - Sales\Ecommerce\Sales Reports\Greg Common Files\Inventory\ODF_master.csv')


inventory_detail_summed = inventory_detail_summed[['MAP', 'Dsc', 'Category 1', 'Category 2', 'SumOfQAV', 'SumOfQOO','PartNo']]
Amazon_profile = Amazon_profile[['CUST$', 'PART#']]
Amazon_profile = Amazon_profile.drop_duplicates(subset='PART#', keep='first', inplace=False)
Amazon_ODF_result = Amazon_ODF_result[['Price', 'Description', 'Reviews', 'Ratings', 'Stock', 'PageURL', 'Error', 'MainPartNumber', 'BuyBox']]

Amazon_ODF_result = Amazon_ODF_result.rename(columns = {'MainPartNumber' : 'Model'})
inventory_detail_summed = inventory_detail_summed.rename(columns={'PartNo': 'Model'})
Amazon_profile = Amazon_profile.rename(columns={'PART#': 'Model'})

ODF_Sku = ODF_Sku.rename(columns = {'SKU' : 'Model'})
ODF_Sku = ODF_Sku.rename(columns = {'MFG' : 'Mfr'})

Part_1 = pd.merge(Amazon_ODF_result, ODF_Sku, how = 'right', on = ['Model'])
Part_2 = pd.merge(Part_1, Amazon_profile, how = 'left', on = ['Model'])
Part_4 = pd.merge(Part_2, inventory_detail_summed, how = 'left', on = ['Model'])


Part_4 = Part_4.rename(columns = {'New' : 'New Skus'})
Part_4 = Part_4.rename(columns = {'BuyBox' : 'Buy Box'})
Part_4 = Part_4.rename(columns = {'PageURL' : 'Page URL'})

Part_4 = Part_4[['Mfr', 'Model', 'ASIN', 'Category 1', 'Category 2', 'Ratings', 'Reviews', 'SumOfQAV', 'SumOfQOO', 'Dsc', 'Buy Box', 'Stock', 'Price', 'CUST$', 'MAP', 'Description', 'Error', 'Page URL']]

Part_4 = Part_4.drop_duplicates(subset = None, keep = 'first')



Part_4.to_csv(r'P:\Dept - Sales\Ecommerce\Sales Reports\Greg Common Files\Mozenda\Dashboards\ODF.csv', index=False)


import pymysql
import codecs
import csv

db = pymysql.connect(host="mozenda-database.ctozypyhj0fo.us-east-1.rds.amazonaws.com",port=3306,
                   user="admin",password="QUsize920908",
                   database="Dashboard",charset="utf8")

def insert(cur, sql, args):
    try:
        cur.execute(sql, args)
    except Exception as e:
        print(e)
        db.rollback()

def read_csv_to_mysql(filename):
    with codecs.open(filename=filename, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        head = next(reader)
        print(head)
        conn = db
        cur = conn.cursor()
        sql = 'insert into Amazon_ODF values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        count = 0
        for item in reader:
            args = tuple(item)
            insert(cur, sql=sql, args=args)
            count += 1

        conn.commit()
        cur.close()
        conn.close()
        print(str(count) + ' rows inserted')

if __name__ == '__main__':
    read_csv_to_mysql(r'P:\Dept - Sales\Ecommerce\Sales Reports\Greg Common Files\Mozenda\Dashboards\ODF.csv')
