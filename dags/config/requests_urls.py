from typing import List
import requests
import pandas as pd
from lxml import html
import time

def parsing_urls(url:str ,Category:str, Num:int, Size:int, Year:int, Month:int) -> List[str] :
    param = dict(categoryNumber=Category, pageNumber=Num, pageSize=Size, saleYear=Year, saleMonth=Month)
    res = requests.get(url, params=param)
    result = html.fromstring(res.text).xpath("//a[@class='bgYUI ico_nWin']/@href")
    return result

def collect_links():
    data_lst = []
    urls = "https://www.yes24.com/Product/Category/MonthWeekBestSeller"
    for year in range(2024, 2025):
        for month in range(1, 13):
            for page in range(1, 10):
                tmp_data = parsing_urls(urls, "001", page, 120, year, month)
                tmp_df = pd.DataFrame(tmp_data, columns=["links"])
                data_lst.append(tmp_df)
                time.sleep(3)
    return data_lst

if __name__ == "__main__" :
    collect_links()
    print(collect_links())