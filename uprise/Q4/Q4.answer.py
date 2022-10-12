import requests
from bs4 import BeautifulSoup
import pandas as pd
from numpy import NaN

from fake_useragent import UserAgent


# S&P500 Symbol list 수집
def get_SnP500_list():
    SnP500_list_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    SnP500_list_html_request = requests.get(SnP500_list_url)
    SnP500_list_html = SnP500_list_html_request.content.decode('utf-8','replace')
    soup = BeautifulSoup(SnP500_list_html, 'html.parser')
    table = soup.select_one('table > tbody').select('tr')
    return [element.text.strip().split('\n')[0] for element in table[1:]]

class CoSnP500:
    
    def __init__(self):
        self.return_df = pd.DataFrame()

    # symbol별 close_price 데이터 수집
    def crawl_company_data(self, company_symbol):
        
        ua = UserAgent()
        headers = {"User-Agent": ua.random}
        search_company_uri = f"https://finance.yahoo.com/quote/{company_symbol}/history?p={company_symbol}"
        res_company_data= requests.get(search_company_uri, headers=headers)
        
        html_company_data = res_company_data.content.decode('utf-8','replace')
        soup = BeautifulSoup(html_company_data, 'html.parser')
        table = soup.select_one('table').select('tr')
        data_array = [list(map(lambda x : str(x)[6:-7], elements.select('span'))) for elements in table]
        company_df = pd.DataFrame(data_array[1:-1], columns=data_array[0])
        
        self.transform_to_return(company_symbol, company_df)
    
    # price 행렬에서 return 행렬로 전환
    def transform_to_return(self, company_symbol, company_df):
        close_df = company_df[['Date', 'Close*']].rename(columns = {'Close*':company_symbol})
        close_df = close_df.fillna(method='ffill').astype({company_symbol:'float'}) # 결측지 채우기/평균값
        close_df_list = close_df.to_dict('list')
        close_prices= close_df_list[company_symbol]
        
        return_prices = []
        for i in range(len(close_prices))[:-1]:
            return_prices += [(close_prices[i] - close_prices[i+1])/close_prices[i+1] - 1]
        
        close_df_list[company_symbol] = return_prices
        close_df_list['Date'] = close_df_list['Date'][:-1]
        close_return_df = pd.DataFrame(close_df_list)
        
        self.join_return_df(close_return_df)

    # 기업 수집 하나당 데이터 프레임 조인
    def join_return_df(self, close_return_df):
        if not self.return_df.equals(pd.DataFrame()):
            self.return_df = pd.merge(self.return_df, close_return_df, on='Date', how='left')
        else:
            self.return_df = close_return_df
    
    # 상관계수 행렬로 변환
    def make_cor_matrix(self):
        self.SnP500_cor_df = self.return_df.corr(method='pearson')
        return self.SnP500_cor_df
    
    # 상관계수 행렬 중 원하는 위치값 추출
    def check_cor_value(self, company_symbol1, company_symbol2):
        return self.SnP500_cor_df[company_symbol2].loc[company_symbol1]
    
SnP500_list = get_SnP500_list()
cosnp500 = CoSnP500()

for company_symbol in SnP500_list[:50]:
    cosnp500.crawl_company_data(company_symbol)
cosnp500.make_cor_matrix()

Q4_bool = "참" if cosnp500.check_cor_value('GOOGL', 'AAPL') > 0.7 else "거짓"
print(f'1번 명제는 {Q4_bool} 입니다.')


