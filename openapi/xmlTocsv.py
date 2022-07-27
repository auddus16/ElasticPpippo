import requests, bs4
import pandas as pd
from lxml import html
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote

url = 'http://apis.data.go.kr/B551182/nonPaymentDamtInfoService/getNonPaymentItemHospDtlList'
serviceKey = 'O+6wCPB3OtdW24LIG9YMrZRTmHf0aPlw0PEDC5eB1To3ozw5x1Sxi7NqBa1M0V0XhqaUd1+wWB8aKqqPNvIvDw=='
params ={'serviceKey' : serviceKey}

response = requests.get(url, params=params).text.encode('utf-8')
xmlobj = bs4.BeautifulSoup(response, 'lxml-xml')

rows = xmlobj.findAll('item')
# print(rows)
# rows    # 디버깅용.
columns = rows[0].find_all()
# columns    # 디버깅용.
# columns[0].name    # 디버깅용.
# columns[0].text    # 디버깅용.
# print(columns)
# 모든 행과 열의 값을 모아 매트릭스로 만들어보자.
rowList = []
nameList = []
columnList = []

rowsLen = len(rows)
for i in range(0, rowsLen):
    columns = rows[i].find_all()

    columnsLen = len(columns)
    for j in range(0, columnsLen):
        # 첫 번째 행 데이터 값 수집 시에만 컬럼 값을 저장한다. (어차피 rows[0], rows[1], ... 모두 컬럼헤더는 동일한 값을 가지기 때문에 매번 반복할 필요가 없다.)
        if i == 0:
            nameList.append(columns[j].name)
        # 컬럼값은 모든 행의 값을 저장해야한다.
        eachColumn = columns[j].text
        columnList.append(eachColumn)
    rowList.append(columnList)
    columnList = []  # 다음 row의 값을 넣기 위해 비워준다. (매우 중요!!)

result = pd.DataFrame(rowList, columns=nameList)
# result.drop('', axis=columns)
result.to_csv("비급여.csv", index=None)

# print(result)