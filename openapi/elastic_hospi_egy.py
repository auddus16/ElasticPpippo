import xmltodict
from elasticsearch import Elasticsearch, helpers
import requests
import json

# serviceKey = 'O+6wCPB3OtdW24LIG9YMrZRTmHf0aPlw0PEDC5eB1To3ozw5x1Sxi7NqBa1M0V0XhqaUd1+wWB8aKqqPNvIvDw=='
serviceKey = 'OiAptfzton/y5ouxQTvEgmDTvAuxGJvqhSQLzMqOiqt3918lbOq22d/jSfW3mfwTD3kOlQuvPIvN2i3XJ4nDHw=='
# encoding : O%2B6wCPB3OtdW24LIG9YMrZRTmHf0aPlw0PEDC5eB1To3ozw5x1Sxi7NqBa1M0V0XhqaUd1%2BwWB8aKqqPNvIvDw%3D%3D
def openAPI_emergency(): # 응급실정보조회
    url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire'
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
    docs = []
    # total_count // 1000 + 1
    for i in range(total_count // 1000 + 1):
        # print(i)
        url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire'
        params = {'serviceKey': serviceKey, 'numOfRows': 1000, 'pageNo': (i+1)}

        response = requests.get(url, params=params).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        # print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))
    # print(xml_dict['response']['body']['items']['item'][0])
        print(i+1)
        # docs = []
        for d in xml_dict['response']['body']['items']['item']:
            res = {'egyType':'응급의료기관'}

            if 'dgidIdName' in d: # 진료과목 자르기
                dgidName = d['dgidIdName'].split(',')
                res['dgidIdName'] = []
                for n in dgidName:
                    res['dgidIdName'].append({'name': n})
                # print(res['dgidIdName'])
            if 'hpid' in d:
                res['hpid'] = d['hpid']
            if 'dutyName' in d:
                res['dutyName'] = d['dutyName']
            if 'dutyAddr' in d:
                res['dutyAddr'] = d['dutyAddr']
            if 'dutyTel1' in d:
                res['dutyTel1'] = d['dutyTel1']
            if 'dutyTel3' in d:
                res['dutyTel3'] = d['dutyTel3']
            if 'hvec' in d:
                res['hvec'] = int(d['hvec'])
            if 'hvoc' in d:
                res['hvoc'] = int(d['hvoc'])
            if 'hvcc' in d:
                res['hvcc'] = int(d['hvcc'])
            if 'hvncc' in d:
                res['hvncc'] = int(d['hvncc'])
            if 'hvccc' in d:
                res['hvccc'] = int(d['hvccc'])
            if 'hvicc' in d:
                res['hvicc'] = int(d['hvicc'])
            if 'hvgc' in d:
                res['hvgc'] = int(d['hvgc'])
            if 'dutyHayn' in d:
                res['dutyHayn'] = int(d['dutyHayn'])
            if 'dutyHano' in d:
                if d['dutyHano'] is None:
                    # print(res)
                    pass
                else:
                    res['dutyHano'] = int(d['dutyHano'])
            if 'dutyInf' in d:
                res['dutyInf'] = d['dutyInf']
            if 'dutyMapimg' in d:
                res['dutyMapimg'] = d['dutyMapimg']
            if 'dutyEryn' in d:
                res['dutyEryn'] = int(d['dutyEryn'])
            if 'dutyTime1c' in d:
                res['dutyTime1c'] = d['dutyTime1c'][:2]+':'+ d['dutyTime1c'][2:] if d['dutyTime1c'][:2] != '24' else '00:'+ d['dutyTime1c'][2:]
            if 'dutyTime2c' in d:
                res['dutyTime2c'] = d['dutyTime2c'][:2]+':'+ d['dutyTime2c'][2:] if d['dutyTime2c'][:2] != '24' else '00:'+ d['dutyTime2c'][2:]
            if 'dutyTime3c' in d:
                res['dutyTime3c'] = d['dutyTime3c'][:2]+':'+ d['dutyTime3c'][2:] if d['dutyTime3c'][:2] != '24' else '00:'+ d['dutyTime3c'][2:]
            if 'dutyTime4c' in d:
                res['dutyTime4c'] = d['dutyTime4c'][:2]+':'+ d['dutyTime4c'][2:] if d['dutyTime4c'][:2] != '24' else '00:'+ d['dutyTime4c'][2:]
            if 'dutyTime5c' in d:
                res['dutyTime5c'] = d['dutyTime5c'][:2]+':'+ d['dutyTime5c'][2:] if d['dutyTime5c'][:2] != '24' else '00:'+ d['dutyTime5c'][2:]
            if 'dutyTime6c' in d:
                res['dutyTime6c'] = d['dutyTime6c'][:2]+':'+ d['dutyTime6c'][2:] if d['dutyTime6c'][:2] != '24' else '00:'+ d['dutyTime6c'][2:]
            if 'dutyTime7c' in d:
                res['dutyTime7c'] = d['dutyTime7c'][:2]+':'+ d['dutyTime7c'][2:] if d['dutyTime7c'][:2] != '24' else '00:'+ d['dutyTime7c'][2:]
            if 'dutyTime8c' in d:
                res['dutyTime8c'] = d['dutyTime8c'][:2]+':'+ d['dutyTime8c'][2:] if d['dutyTime8c'][:2] != '24' else '00:'+ d['dutyTime8c'][2:]
            if 'dutyTime1s' in d:
                res['dutyTime1s'] = d['dutyTime1s'][:2]+':'+ d['dutyTime1s'][2:] if d['dutyTime1s'][:2] != '24' else '00:'+ d['dutyTime1s'][2:]
            if 'dutyTime2s' in d:
                res['dutyTime2s'] = d['dutyTime2s'][:2]+':'+ d['dutyTime2s'][2:] if d['dutyTime2s'][:2] != '24' else '00:'+ d['dutyTime2s'][2:]
            if 'dutyTime3s' in d:
                res['dutyTime3s'] = d['dutyTime3s'][:2]+':'+ d['dutyTime3s'][2:] if d['dutyTime3s'][:2] != '24' else '00:'+ d['dutyTime3s'][2:]
            if 'dutyTime4s' in d:
                res['dutyTime4s'] = d['dutyTime4s'][:2]+':'+ d['dutyTime4s'][2:] if d['dutyTime4s'][:2] != '24' else '00:'+ d['dutyTime4s'][2:]
            if 'dutyTime5s' in d:
                res['dutyTime5s'] = d['dutyTime5s'][:2]+':'+ d['dutyTime5s'][2:] if d['dutyTime5s'][:2] != '24' else '00:'+ d['dutyTime5s'][2:]
            if 'dutyTime6s' in d:
                res['dutyTime6s'] = d['dutyTime6s'][:2]+':'+ d['dutyTime6s'][2:] if d['dutyTime6s'][:2] != '24' else '00:'+ d['dutyTime6s'][2:]
            if 'dutyTime7s' in d:
                res['dutyTime7s'] = d['dutyTime7s'][:2]+':'+ d['dutyTime7s'][2:] if d['dutyTime7s'][:2] != '24' else '00:'+ d['dutyTime7s'][2:]
            if 'dutyTime8s' in d:
                res['dutyTime8s'] = d['dutyTime8s'][:2]+':'+ d['dutyTime8s'][2:] if d['dutyTime8s'][:2] != '24' else '00:'+ d['dutyTime8s'][2:]

            if 'MKioskTy25' in d and d['MKioskTy25'] == 'Y':
                res['MKioskTy25'] = '응급실(Emergency gate keeper)'
            if 'MKioskTy1' in d and d['MKioskTy1'] == 'Y':
                res['MKioskTy1'] = '뇌출혈수술'
            if 'MKioskTy2' in d and d['MKioskTy2'] == 'Y':
                res['MKioskTy2'] = '뇌경색의재관류'
            if 'MKioskTy3' in d and d['MKioskTy3'] == 'Y':
                res['MKioskTy3'] = '심근경색의재관류'
            if 'MKioskTy4' in d and d['MKioskTy4'] == 'Y':
                res['MKioskTy4'] = '복부손상의수술'
            if 'MKioskTy5' in d and d['MKioskTy5'] == 'Y':
                res['MKioskTy5'] = '사지접합의수술'
            if 'MKioskTy6' in d and d['MKioskTy6'] == 'Y':
                res['MKioskTy6'] = '응급내시경'
            if 'MKioskTy7' in d and d['MKioskTy7'] == 'Y':
                res['MKioskTy7'] = '응급투석'
            if 'MKioskTy8' in d and d['MKioskTy8'] == 'Y':
                res['MKioskTy8'] = '조산산모'
            if 'MKioskTy9' in d and d['MKioskTy9'] == 'Y':
                res['MKioskTy9'] = '정신질환자'
            if 'MKioskTy10' in d and d['MKioskTy10'] == 'Y':
                res['MKioskTy10'] = '신생아'
            if 'MKioskTy11' in d and d['MKioskTy11'] == 'Y':
                res['MKioskTy11'] = '중증화상'

            if 'wgs84Lon' in d and 'wgs84Lat' in d:
                res['location'] = {"lat": float(d['wgs84Lat']), "lon": float(d['wgs84Lon'])}

            if 'hpbdn' in d:
                res['hpbdn'] = int(d['hpbdn'])
            if 'hpccuyn' in d:
                res['hpccuyn'] = int(d['hpccuyn'])
            if 'hpcuyn' in d:
                res['hpcuyn'] = int(d['hpcuyn'])
            if 'hperyn' in d:
                res['hperyn'] = int(d['hperyn'])
            if 'hpgryn' in d:
                res['hpgryn'] = int(d['hpgryn'])
            if 'hpicuyn' in d:
                res['hpicuyn'] = int(d['hpicuyn'])
            if 'hpnicuyn' in d:
                res['hpnicuyn'] = int(d['hpnicuyn'])
            if 'hpopyn' in d:
                res['hpopyn'] = int(d['hpopyn'])

            make_doc(docs, res)
    # print(docs)

    helpers.bulk(es, docs, raise_on_error=False)
    # print('벌크함.') raise_on_error=False
def make_doc(docs, res):
    doc = {
        "_index": "hospi_egy_info"
    }
    doc['_source'] = res
    docs.append(doc)

# openAPI_emergency()

def openAPI_strm(): # 외상센터정보조회
    url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getStrmBassInfoInqire'
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
    docs = []
    # total_count // 1000 + 1
    for i in range(total_count // 10 + 1):
        # print(i)
        url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire'
        params = {'serviceKey': serviceKey, 'numOfRows': 10, 'pageNo': (i+1)}

        response = requests.get(url, params=params).text.encode('utf-8')

        decode_data = response.decode('utf-8')
        # print(decode_data)

        xml_parse = xmltodict.parse(decode_data)  # string인 xml 파싱
        xml_dict = json.loads(json.dumps(xml_parse))
    # print(xml_dict['response']['body']['items']['item'][0])
        print(i+1)
        # docs = []
        for d in xml_dict['response']['body']['items']['item']:
            res = {'egyType':'외상센터'}

            if 'dgidIdName' in d: # 진료과목 자르기
                dgidName = d['dgidIdName'].split(',')
                res['dgidIdName'] = []
                for n in dgidName:
                    res['dgidIdName'].append({'name': n})
                # print(res['dgidIdName'])
            if 'hpid' in d:
                res['hpid'] = d['hpid']
            if 'dutyName' in d:
                res['dutyName'] = d['dutyName']
            if 'dutyAddr' in d:
                res['dutyAddr'] = d['dutyAddr']
            if 'dutyTel1' in d:
                res['dutyTel1'] = d['dutyTel1']
            if 'dutyTel3' in d:
                res['dutyTel3'] = d['dutyTel3']
            if 'hvec' in d:
                res['hvec'] = int(d['hvec'])
            if 'hvoc' in d:
                res['hvoc'] = int(d['hvoc'])
            if 'hvcc' in d:
                res['hvcc'] = int(d['hvcc'])
            if 'hvncc' in d:
                res['hvncc'] = int(d['hvncc'])
            if 'hvccc' in d:
                res['hvccc'] = int(d['hvccc'])
            if 'hvicc' in d:
                res['hvicc'] = int(d['hvicc'])
            if 'hvgc' in d:
                res['hvgc'] = int(d['hvgc'])
            if 'dutyHayn' in d:
                res['dutyHayn'] = int(d['dutyHayn'])
            if 'dutyHano' in d:
                if d['dutyHano'] is None:
                    # print(res)
                    pass
                else:
                    res['dutyHano'] = int(d['dutyHano'])
            if 'dutyInf' in d:
                res['dutyInf'] = d['dutyInf']
            if 'dutyMapimg' in d:
                res['dutyMapimg'] = d['dutyMapimg']
            if 'dutyEryn' in d:
                res['dutyEryn'] = int(d['dutyEryn'])
            if 'dutyTime1c' in d:
                res['dutyTime1c'] = d['dutyTime1c'][:2]+':'+ d['dutyTime1c'][2:] if d['dutyTime1c'][:2] != '24' else '00:'+ d['dutyTime1c'][2:]
            if 'dutyTime2c' in d:
                res['dutyTime2c'] = d['dutyTime2c'][:2]+':'+ d['dutyTime2c'][2:] if d['dutyTime2c'][:2] != '24' else '00:'+ d['dutyTime2c'][2:]
            if 'dutyTime3c' in d:
                res['dutyTime3c'] = d['dutyTime3c'][:2]+':'+ d['dutyTime3c'][2:] if d['dutyTime3c'][:2] != '24' else '00:'+ d['dutyTime3c'][2:]
            if 'dutyTime4c' in d:
                res['dutyTime4c'] = d['dutyTime4c'][:2]+':'+ d['dutyTime4c'][2:] if d['dutyTime4c'][:2] != '24' else '00:'+ d['dutyTime4c'][2:]
            if 'dutyTime5c' in d:
                res['dutyTime5c'] = d['dutyTime5c'][:2]+':'+ d['dutyTime5c'][2:] if d['dutyTime5c'][:2] != '24' else '00:'+ d['dutyTime5c'][2:]
            if 'dutyTime6c' in d:
                res['dutyTime6c'] = d['dutyTime6c'][:2]+':'+ d['dutyTime6c'][2:] if d['dutyTime6c'][:2] != '24' else '00:'+ d['dutyTime6c'][2:]
            if 'dutyTime7c' in d:
                res['dutyTime7c'] = d['dutyTime7c'][:2]+':'+ d['dutyTime7c'][2:] if d['dutyTime7c'][:2] != '24' else '00:'+ d['dutyTime7c'][2:]
            if 'dutyTime8c' in d:
                res['dutyTime8c'] = d['dutyTime8c'][:2]+':'+ d['dutyTime8c'][2:] if d['dutyTime8c'][:2] != '24' else '00:'+ d['dutyTime8c'][2:]
            if 'dutyTime1s' in d:
                res['dutyTime1s'] = d['dutyTime1s'][:2]+':'+ d['dutyTime1s'][2:] if d['dutyTime1s'][:2] != '24' else '00:'+ d['dutyTime1s'][2:]
            if 'dutyTime2s' in d:
                res['dutyTime2s'] = d['dutyTime2s'][:2]+':'+ d['dutyTime2s'][2:] if d['dutyTime2s'][:2] != '24' else '00:'+ d['dutyTime2s'][2:]
            if 'dutyTime3s' in d:
                res['dutyTime3s'] = d['dutyTime3s'][:2]+':'+ d['dutyTime3s'][2:] if d['dutyTime3s'][:2] != '24' else '00:'+ d['dutyTime3s'][2:]
            if 'dutyTime4s' in d:
                res['dutyTime4s'] = d['dutyTime4s'][:2]+':'+ d['dutyTime4s'][2:] if d['dutyTime4s'][:2] != '24' else '00:'+ d['dutyTime4s'][2:]
            if 'dutyTime5s' in d:
                res['dutyTime5s'] = d['dutyTime5s'][:2]+':'+ d['dutyTime5s'][2:] if d['dutyTime5s'][:2] != '24' else '00:'+ d['dutyTime5s'][2:]
            if 'dutyTime6s' in d:
                res['dutyTime6s'] = d['dutyTime6s'][:2]+':'+ d['dutyTime6s'][2:] if d['dutyTime6s'][:2] != '24' else '00:'+ d['dutyTime6s'][2:]
            if 'dutyTime7s' in d:
                res['dutyTime7s'] = d['dutyTime7s'][:2]+':'+ d['dutyTime7s'][2:] if d['dutyTime7s'][:2] != '24' else '00:'+ d['dutyTime7s'][2:]
            if 'dutyTime8s' in d:
                res['dutyTime8s'] = d['dutyTime8s'][:2]+':'+ d['dutyTime8s'][2:] if d['dutyTime8s'][:2] != '24' else '00:'+ d['dutyTime8s'][2:]

            if 'MKioskTy25' in d and d['MKioskTy25'] == 'Y':
                res['MKioskTy25'] = '응급실(Emergency gate keeper)'
            if 'MKioskTy1' in d and d['MKioskTy1'] == 'Y':
                res['MKioskTy1'] = '뇌출혈수술'
            if 'MKioskTy2' in d and d['MKioskTy2'] == 'Y':
                res['MKioskTy2'] = '뇌경색의재관류'
            if 'MKioskTy3' in d and d['MKioskTy3'] == 'Y':
                res['MKioskTy3'] = '심근경색의재관류'
            if 'MKioskTy4' in d and d['MKioskTy4'] == 'Y':
                res['MKioskTy4'] = '복부손상의수술'
            if 'MKioskTy5' in d and d['MKioskTy5'] == 'Y':
                res['MKioskTy5'] = '사지접합의수술'
            if 'MKioskTy6' in d and d['MKioskTy6'] == 'Y':
                res['MKioskTy6'] = '응급내시경'
            if 'MKioskTy7' in d and d['MKioskTy7'] == 'Y':
                res['MKioskTy7'] = '응급투석'
            if 'MKioskTy8' in d and d['MKioskTy8'] == 'Y':
                res['MKioskTy8'] = '조산산모'
            if 'MKioskTy9' in d and d['MKioskTy9'] == 'Y':
                res['MKioskTy9'] = '정신질환자'
            if 'MKioskTy10' in d and d['MKioskTy10'] == 'Y':
                res['MKioskTy10'] = '신생아'
            if 'MKioskTy11' in d and d['MKioskTy11'] == 'Y':
                res['MKioskTy11'] = '중증화상'

            if 'wgs84Lon' in d and 'wgs84Lat' in d:
                res['location'] = {"lat": float(d['wgs84Lat']), "lon": float(d['wgs84Lon'])}

            if 'hpbdn' in d:
                res['hpbdn'] = int(d['hpbdn'])
            if 'hpccuyn' in d:
                res['hpccuyn'] = int(d['hpccuyn'])
            if 'hpcuyn' in d:
                res['hpcuyn'] = int(d['hpcuyn'])
            if 'hperyn' in d:
                res['hperyn'] = int(d['hperyn'])
            if 'hpgryn' in d:
                res['hpgryn'] = int(d['hpgryn'])
            if 'hpicuyn' in d:
                res['hpicuyn'] = int(d['hpicuyn'])
            if 'hpnicuyn' in d:
                res['hpnicuyn'] = int(d['hpnicuyn'])
            if 'hpopyn' in d:
                res['hpopyn'] = int(d['hpopyn'])

            make_doc(docs, res)

    helpers.bulk(es, docs, raise_on_error=False)

openAPI_emergency()
openAPI_strm()

# 응급실기본정보 http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytBassInfoInqire