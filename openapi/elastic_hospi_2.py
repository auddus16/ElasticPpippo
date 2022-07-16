import xmltodict
from elasticsearch import Elasticsearch, helpers
import requests
import json

serviceKey = 'O+6wCPB3OtdW24LIG9YMrZRTmHf0aPlw0PEDC5eB1To3ozw5x1Sxi7NqBa1M0V0XhqaUd1+wWB8aKqqPNvIvDw=='

def openAPI(): # 평가등급조회
    url = 'http://apis.data.go.kr/B551182/hospAsmInfoService/getHospAsmInfo'
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
        url = 'http://apis.data.go.kr/B551182/hospAsmInfoService/getHospAsmInfo'
        params = {'serviceKey': serviceKey, 'numOfRows': 1000, 'pageNo': (i+1)}

        response = requests.get(url, params=params).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        # print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))
    # print(xml_dict['response']['body']['items']['item'][0])
        print(i+1)

        for d in xml_dict['response']['body']['items']['item']:
            # print(d['XPos'])
            docs = []
            if 'ykiho' not in d: # 요양기호 없으면 break
                break
            res = {'ykiho': d['ykiho']}
            if 'asmGrd01' in d:
                res['asmGrdType'] = '01'
                res['asmGrd'] = d['asmGrd01']
                make_doc(docs, res)
            if 'asmGrd02' in d:
                res['asmGrdType'] = '02'
                res['asmGrd'] = d['asmGrd02']
                make_doc(docs, res)
            if 'asmGrd03' in d:
                res['asmGrdType'] = '03'
                res['asmGrd'] = d['asmGrd03']
                make_doc(docs, res)
            if 'asmGrd04' in d:
                res['asmGrdType'] = '04'
                res['asmGrd'] = d['asmGrd04']
                make_doc(docs, res)
            if 'asmGrd05' in d:
                res['asmGrdType'] = '05'
                res['asmGrd'] = d['asmGrd05']
                make_doc(docs, res)
            if 'asmGrd06' in d:
                res['asmGrdType'] = '06'
                res['asmGrd'] = d['asmGrd06']
                make_doc(docs, res)
            if 'asmGrd07' in d:
                res['asmGrdType'] = '07'
                res['asmGrd'] = d['asmGrd07']
                make_doc(docs, res)
            if 'asmGrd08' in d:
                res['asmGrdType'] = '08'
                res['asmGrd'] = d['asmGrd08']
                make_doc(docs, res)
            if 'asmGrd09' in d:
                res['asmGrdType'] = '09'
                res['asmGrd'] = d['asmGrd09']
                make_doc(docs, res)
            if 'asmGrd10' in d:
                res['asmGrdType'] = '10'
                res['asmGrd'] = d['asmGrd10']
                make_doc(docs, res)
            if 'asmGrd11' in d:
                res['asmGrdType'] = '11'
                res['asmGrd'] = d['asmGrd11']
                make_doc(docs, res)
            if 'asmGrd12' in d:
                res['asmGrdType'] = '12'
                res['asmGrd'] = d['asmGrd12']
                make_doc(docs, res)
            if 'asmGrd13' in d:
                res['asmGrdType'] = '13'
                res['asmGrd'] = d['asmGrd13']
                make_doc(docs, res)
            if 'asmGrd14' in d:
                res['asmGrdType'] = '14'
                res['asmGrd'] = d['asmGrd14']
                make_doc(docs, res)
            if 'asmGrd15' in d:
                res['asmGrdType'] = '15'
                res['asmGrd'] = d['asmGrd15']
                make_doc(docs, res)
            if 'asmGrd16' in d:
                res['asmGrdType'] = '16'
                res['asmGrd'] = d['asmGrd16']
                make_doc(docs, res)
            if 'asmGrd17' in d:
                res['asmGrdType'] = '17'
                res['asmGrd'] = d['asmGrd17']
                make_doc(docs, res)
            if 'asmGrd18' in d:
                res['asmGrdType'] = '18'
                res['asmGrd'] = d['asmGrd18']
                make_doc(docs, res)
            if 'asmGrd19' in d:
                res['asmGrdType'] = '19'
                res['asmGrd'] = d['asmGrd19']
                make_doc(docs, res)
            if 'asmGrd20' in d:
                res['asmGrdType'] = '20'
                res['asmGrd'] = d['asmGrd20']
                make_doc(docs, res)
            if 'asmGrd21' in d:
                res['asmGrdType'] = '21'
                res['asmGrd'] = d['asmGrd21']
                make_doc(docs, res)
            if 'asmGrd22' in d:
                res['asmGrdType'] = '22'
                res['asmGrd'] = d['asmGrd22']
                make_doc(docs, res)
            if 'asmGrd23' in d:
                res['asmGrdType'] = '23'
                res['asmGrd'] = d['asmGrd23']
                make_doc(docs, res)
            print(docs)
            helpers.bulk(es, docs)

def make_doc(docs, res):
    doc = {
        "_index": "hospi_asm_info_3",
        "_source": {
            "ykiho": res['ykiho'],
            "asmGrdType": res['asmGrdType'],
            "asmGrd": res['asmGrd']
        }
    }
    docs.append(doc)

    # print(response)
# openAPI()

def openAPI_excl_asm_Info(): # 우수기관평가정보서비스
    url = 'http://apis.data.go.kr/B551182/exclInstHospAsmInfoService/getExclInstHospAsmInfo'
    global serviceKey
    params = {'serviceKey': serviceKey, 'numOfRows': 1}

    response = requests.get(url, params=params).text.encode('utf-8')

    decode_data = response.decode('utf-8')
    print(decode_data)

    xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
    xml_dict = json.loads(json.dumps(xml_parse))

    total_count = int(xml_dict['response']['body']['totalCount'])
    print('total count: ' + str(total_count))

    es = Elasticsearch(
        cloud_id='buyornot:YXAtbm9ydGhlYXN0LTIuYXdzLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkZDcyODFhN2RiYTVhNDQ5MjhlOTEzMjBlMzllZjUxNTMkYmUzZjA1NjgyZDU5NGNmZmJkNTYxNjM5OWNlN2FiNTc=',
        basic_auth=('elastic', 'Orf5PC90BVmMMuVU5cKoTyrs'),
    )

    for i in range(total_count // 1000 + 1):
        url = 'http://apis.data.go.kr/B551182/exclInstHospAsmInfoService/getExclInstHospAsmInfo'
        params = {'serviceKey': serviceKey, 'numOfRows': 1000, 'pageNo': (i + 1)}

        response = requests.get(url, params=params).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        # print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))
        # print(xml_dict['response']['body']['items']['item'][0])
        print(i + 1)
        docs = []
        for d in xml_dict['response']['body']['items']['item']:
            # print(d['XPos'])
            doc = {
                "_index": "hospi_excl_asm_info",
                "_source": {
                    "ykiho": d['ykiho'] if 'ykiho' in d else '없음',
                    "yadmNm": d['yadmNm'] if 'yadmNm' in d else '없음',
                    "asmNm": d['asmNm'] if 'asmNm' in d else '없음',
                    "asmGrd": d['asmGrd'] if 'asmGrd' in d else '없음',
                    "asmGrdNm": d['asmGrdNm'] if 'asmGrdNm' in d else '없음',
                }
            }
            docs.append(doc)
        helpers.bulk(es, docs)
# openAPI_excl_asm_Info()

def openAPI_npay_Info(): # 비급여
    url = 'http://apis.data.go.kr/B551182/nonPaymentDamtInfoService/getNonPaymentItemHospDtlList'
    global serviceKey
    params = {'serviceKey': serviceKey, 'numOfRows': 1}

    response = requests.get(url, params=params).text.encode('utf-8')

    decode_data = response.decode('utf-8')
    print(decode_data)

    xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
    xml_dict = json.loads(json.dumps(xml_parse))

    total_count = int(xml_dict['response']['body']['totalCount'])
    print('total count: ' + str(total_count))

    es = Elasticsearch(
        cloud_id='buyornot:YXAtbm9ydGhlYXN0LTIuYXdzLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkZDcyODFhN2RiYTVhNDQ5MjhlOTEzMjBlMzllZjUxNTMkYmUzZjA1NjgyZDU5NGNmZmJkNTYxNjM5OWNlN2FiNTc=',
        basic_auth=('elastic', 'Orf5PC90BVmMMuVU5cKoTyrs'),
    )

    for i in range(total_count // 1000 + 1):
        url = 'http://apis.data.go.kr/B551182/nonPaymentDamtInfoService/getNonPaymentItemHospDtlList'
        params = {'serviceKey': serviceKey, 'numOfRows': 1000, 'pageNo': (i + 1)}

        response = requests.get(url, params=params).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        # print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))
        # print(xml_dict['response']['body']['items']['item'][0])
        print(i + 1)
        docs = []
        for d in xml_dict['response']['body']['items']['item']:
            # print(d['XPos'])
            doc = {
                "_index": "hospi_npy_info2",
                "_source": {
                    "ykiho": d['ykiho'] if 'ykiho' in d else '없음',
                    "yadmNm": d['yadmNm'] if 'yadmNm' in d else '없음',
                    "npayCd": d['npayCd'] if 'npayCd' in d else '없음',
                    "npayKorNm": d['npayKorNm'] if 'npayKorNm' in d else '없음',
                    "yadmNpayCdNm": d['yadmNpayCdNm'] if 'yadmNpayCdNm' in d else '없음',
                    "adtFrDd": d['adtFrDd'] if 'adtFrDd' in d else '00000101',
                    "adtEndDd": d['adtEndDd'] if 'adtEndDd' in d else '99991231',
                    "curAmt": int(d['curAmt']) if 'curAmt' in d else 0,
                }
            }
            docs.append(doc)
        helpers.bulk(es, docs)
# openAPI_npay_Info()

# 응급실기본정보 http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire