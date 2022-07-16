import xmltodict
from elasticsearch import Elasticsearch, helpers
import requests
import json

serviceKey = 'O+6wCPB3OtdW24LIG9YMrZRTmHf0aPlw0PEDC5eB1To3ozw5x1Sxi7NqBa1M0V0XhqaUd1+wWB8aKqqPNvIvDw=='

def openAPI(): # 병원정보조회
    url = 'http://apis.data.go.kr/B551182/hospInfoService1/getHospBasisList1'
    global serviceKey
    params = {'serviceKey': serviceKey, 'numOfRows':1}

    response = requests.get(url, params=params).text.encode('utf-8')

    decode_data = response.decode('utf-8')
    print(decode_data)

    xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
    xml_dict = json.loads(json.dumps(xml_parse))

    total_count = int(xml_dict['response']['body']['totalCount'])
    print('total count: '+str(total_count))

    es = Elasticsearch(
        cloud_id='buyornot:YXAtbm9ydGhlYXN0LTIuYXdzLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkZDcyODFhN2RiYTVhNDQ5MjhlOTEzMjBlMzllZjUxNTMkYmUzZjA1NjgyZDU5NGNmZmJkNTYxNjM5OWNlN2FiNTc=',
        basic_auth=('elastic', 'Orf5PC90BVmMMuVU5cKoTyrs'),
    )

    for i in range(total_count//1000+1):
        url = 'http://apis.data.go.kr/B551182/hospInfoService1/getHospBasisList1'
        params = {'serviceKey': serviceKey, 'numOfRows': 1000, 'pageNo': (i+1)}

        response = requests.get(url, params=params).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        # print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))
    # print(xml_dict['response']['body']['items']['item'][0])
        print(i+1)
        docs = []
        for d in xml_dict['response']['body']['items']['item']:
            # print(d['XPos'])
            doc = {
                "_index" : "hospi_info",
                "_source" : {
                    "ykiho" : d['ykiho'] if 'ykiho' in d else '없음',
                    "yadmNm" : d['yadmNm'] if 'yadmNm' in d else '없음',
                    "clCd" : d['clCd'] if 'clCd' in d else '없음',
                    "clCdNm" : d['clCdNm'] if 'clCdNm'in d else '없음',
                    "sidoCd" : d['sidoCd'] if 'sidoCd'in d else '없음',
                    "sidoCdNm": d['sidoCdNm'] if 'sidoCdNm'in d else '없음',
                    "sgguCd": d['sgguCd'] if 'sgguCd'in d else '없음',
                    "sgguCdNm": d['sgguCdNm'] if 'sgguCdNm'in d else '없음',
                    "emdongNm": d['emdongNm'] if 'emdongNm' in d else '없음',
                    "addr": d['addr'] if 'addr'in d else '없음',
                    "telno": d['telno'] if 'telno'in d else '없음',
                    "location": {
                        "lat": float(d['YPos']) if 'YPos'in d else float(0),
                        "lon": float(d['XPos']) if 'XPos'in d else float(0)
                    }
                }
            }
            docs.append(doc)
        helpers.bulk(es, docs, raise_on_error=False)
    # print(response)
# openAPI()

def get_ykiho(): # 전체 병원 요양기호 리스트 반환
    es = Elasticsearch(
        cloud_id='buyornot:YXAtbm9ydGhlYXN0LTIuYXdzLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkZDcyODFhN2RiYTVhNDQ5MjhlOTEzMjBlMzllZjUxNTMkYmUzZjA1NjgyZDU5NGNmZmJkNTYxNjM5OWNlN2FiNTc=',
        basic_auth=('elastic', 'Orf5PC90BVmMMuVU5cKoTyrs'),
    )
    ykiho = []
    for i in range(76):
        if i == 0:
            res = es.search(index='hospi_info', sort=[{'ykiho': 'asc'}], query={
                'match_all': {}
            }, source=['ykiho', 'yadmNm'], size=1000)
            for j in range(1000):
                ykiho.append((res['hits']['hits'][j]['_source']['yadmNm'], res['hits']['hits'][j]['_source']['ykiho']))
        elif i == 75:
            res = es.search(index='hospi_info', sort=[{'ykiho': 'asc'}], query={
                'match_all': {}
            }, source=['ykiho', 'yadmNm'], size=669, search_after=[ykiho[-1][1]])

            for j in range(669):
                ykiho.append((res['hits']['hits'][j]['_source']['yadmNm'], res['hits']['hits'][j]['_source']['ykiho']))
        else:
            res = es.search(index='hospi_info', sort=[{'ykiho': 'asc'}], query={
                'match_all': {}
            }, source=['ykiho', 'yadmNm'], size=1000, search_after=[ykiho[-1][1]])

            for j in range(1000):
                ykiho.append((res['hits']['hits'][j]['_source']['yadmNm'], res['hits']['hits'][j]['_source']['ykiho']))

    # print(res['hits']['hits']['_source']['ykiho'])
    # print(ykiho)
    print(len(ykiho))
    return ykiho

def openAPI_detail_info(): # 의료기관별상세조회 -> 진료과목 조회

    ykiho = get_ykiho()
    url = 'https://apis.data.go.kr/B551182/MadmDtlInfoService/getDgsbjtInfo'
    global serviceKey

    es = Elasticsearch(
        cloud_id='buyornot:YXAtbm9ydGhlYXN0LTIuYXdzLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkZDcyODFhN2RiYTVhNDQ5MjhlOTEzMjBlMzllZjUxNTMkYmUzZjA1NjgyZDU5NGNmZmJkNTYxNjM5OWNlN2FiNTc=',
        basic_auth=('elastic', 'Orf5PC90BVmMMuVU5cKoTyrs'),
    )
    for i in range(67850, len(ykiho)):
        y = ykiho[i]
        print(str(i)+'번째' + y[1])
        params = {'serviceKey': 'wWkVNpiverZP8J5P5j+9XSl5RtXZpgfMrMHiOyK8ENUUG4S9MF+nZLREwDdl6yu/gGkYQgBT+/kh4X7nqqoLDg==', 'ykiho': y[1], 'numOfRows': 100}

        response = requests.get(url, params=params, verify=False).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))

        total_count = int(xml_dict['response']['body']['totalCount'])
        print('total count: ' + str(total_count))
        if total_count == 0:
            continue
        docs = []
        d = xml_dict['response']['body']['items']['item']
        if total_count == 1:
            doc = {
                "_index": "hospi_detail_info",
                "_source": {
                    "ykiho": y[1],
                    "dgsbjtCd": d['dgsbjtCd'] if 'dgsbjtCd' in d else '없음',
                    "dgsbjtCdNm": d['dgsbjtCdNm'] if 'dgsbjtCdNm' in d else '없음',
                    "dgsbjtPrSdrCnt": d['dgsbjtPrSdrCnt'] if 'dgsbjtPrSdrCnt' in d else '0',
                    "cdiagDrCnt": d['cdiagDrCnt'] if 'cdiagDrCnt' in d else '0'
                }
            }
            docs.append(doc)

        else:
            for d in xml_dict['response']['body']['items']['item']:
                # print(d)
                if type(d) is dict:

                    doc = {
                        "_index": "hospi_detail_info",
                        "_source": {
                            "ykiho": y[1],
                            "dgsbjtCd": d['dgsbjtCd'] if 'dgsbjtCd' in d else '없음',
                            "dgsbjtCdNm": d['dgsbjtCdNm'] if 'dgsbjtCdNm' in d else '없음',
                            "dgsbjtPrSdrCnt": d['dgsbjtPrSdrCnt'] if 'dgsbjtPrSdrCnt' in d else '0',
                            "cdiagDrCnt": d['cdiagDrCnt'] if 'cdiagDrCnt' in d else '0'
                        }
                    }
                    print('dododododo')
                    print(doc)
                    docs.append(doc)

        print('docs이다')
        print(docs)
        helpers.bulk(es, docs, raise_on_error=False)

        print('벌크함')
        # for i in range(total_count):
        #     docs = []
        #     for d in xml_dict['response']['body']['items']:
        #         # print(d['XPos'])
        #         doc = {
        #             "_index": "hospi_detail_info",
        #             "_source": {
        #                 "ykiho": y[1],
        #                 "dgsbjtCd": d['item']['dgsbjtCd'] if 'dgsbjtCd' in d else '없음',
        #                 "dgsbjtCdNm": d['item']['dgsbjtCdNm'] if 'dgsbjtCdNm' in d else '없음',
        #                 "dgsbjtPrSdrCnt": d['item']['dgsbjtPrSdrCnt'] if 'dgsbjtPrSdrCnt' in d else '없음',
        #                 "cdiagDrCnt": int(d['cdiagDrCnt']) if 'curAmt' in d else 0
        #             }
        #         }
        #         docs.append(doc)
        #     helpers.bulk(es, docs)
# get_ykiho()

openAPI_detail_info()

def test():
    url = 'https://apis.data.go.kr/B551182/MadmDtlInfoService/getDgsbjtInfo?ServiceKey=O%2B6wCPB3OtdW24LIG9YMrZRTmHf0aPlw0PEDC5eB1To3ozw5x1Sxi7NqBa1M0V0XhqaUd1%2BwWB8aKqqPNvIvDw%3D%3D&ykiho=JDQ4MTAxMiM1MSMkMSMkMCMkMTMkMzgxNzAyIzIxIyQxIyQ1IyQ5MiQzNjEyMjIjNjEjJDEjJDgjJDgz'
    global serviceKey

    es = Elasticsearch(
        cloud_id='buyornot:YXAtbm9ydGhlYXN0LTIuYXdzLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkZDcyODFhN2RiYTVhNDQ5MjhlOTEzMjBlMzllZjUxNTMkYmUzZjA1NjgyZDU5NGNmZmJkNTYxNjM5OWNlN2FiNTc=',
        basic_auth=('elastic', 'Orf5PC90BVmMMuVU5cKoTyrs'),
    )
    # params = {'serviceKey': serviceKey, 'ykiho': y[1], 'numOfRows': 100}

    response = requests.get(url, verify=False).text.encode('utf-8')

    decode_data = response.decode('utf-8')
    print(decode_data)

    xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
    xml_dict = json.loads(json.dumps(xml_parse))

    total_count = int(xml_dict['response']['body']['totalCount'])
    print(xml_dict)
    print(xml_dict['response']['body']['items']['item'])
# test()

def test2(): #2523
    ykiho = get_ykiho()
    # print(ykiho.index('JDQ4MTAxMiM1MSMkMSMkMCMkODIkMzgxMzUxIzExIyQxIyQzIyQwMyQzNjEwMDIjNjEjJDEjJDgjJDgz'))

    for i in range(len(ykiho)):
        if ykiho[i][1] == 'JDQ4MTAxMiM1MSMkMSMkMCMkODIkMzgxMzUxIzExIyQxIyQzIyQwMyQzNjEwMDIjNjEjJDEjJDgjJDgz':
            print(i)

# test2()