# import pymysql
# from datetime import datetime
# import schedule
# import time
#
# from reception import reception
from main_reception import start
from main_reception import end
import requests
import pymysql
from flask import Flask, request, json, jsonify

app = Flask(__name__)

def dbconnect():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
    return conn

def send_start_api(rider_id, oper_id, time, address, company, state):
    API_HOST = "http://127.0.0.1:8091/reception/start"
    url = API_HOST
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    body = {
        "rider_id": rider_id,
        "oper_id": oper_id,
        "start_time": time,
        "address": address,
        "request_company": company
        }
    response = requests.post(url, headers=headers, data=json.dumps(body, ensure_ascii=False, indent="\t"))
    print("response status %r" % response.status_code)
    print("response text %r" % response.text)

def send_end_api(rider_id, oper_id, time):
    API_HOST = "http://127.0.0.1:8091/reception/end"
    url = API_HOST
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    body = {
        "rider_id": rider_id,
        "oper_id": oper_id,
        "end_time": time
    }
    response = requests.post(url, headers=headers, data=json.dumps(body, ensure_ascii=False, indent="\t"))
    print("response status %r" % response.status_code)
    print("response text %r" % response.text)


@app.route("/send", methods=['POST'])

def versionCheck():
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logic.s_info")
    result = cursor.fetchall()
    data_count = len(result)
    print(f"데이터 갯수: {data_count}")

    # start와 end 구분
    sql = f"""
              select
                  rider_id,
                  oper_id,
                  DATE_FORMAT(start_time, '%Y-%m-%d %h:%m:%s') as 'time',
                  address as 'address',
                  request_company as 'company',
                  'start' as 'state'
              from
                  logic.s_info
              union all
              select
                  rider_id as 'rider_id',
                  oper_id as 'oper_id',
                  DATE_FORMAT(end_time, '%Y-%m-%d %h:%m:%s') as 'time',
                  address as 'address',
                  request_company as 'company',
                  'end' as 'state'
              from
                  logic.s_info
              order by time asc;
          """
    cursor.execute(sql)
    conn.commit()
    resultu = cursor.fetchall()

    for row in resultu:
        rider_id, oper_id, time, address, company, state = row
        if state == 'start':
            # print('수신 시작')
            # /reception/start API 호출(운행ID, 기사ID, 시작시간, 주소, 요청사명)
            # json형태로 가공.
            send_start_api(rider_id, oper_id, time, address, company, state)

        else:
            # print('수신 종료')
            # /reception/end API 호출(운행ID, 기사ID, 종료시간)
            # json형태로 가공.
            send_end_api(rider_id, oper_id, time)

    response = {
        "result": "ok"
    }
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8090)


try:
    connection = dbconnect()
    if connection:
        print("DB 접속 완료")
    else:
        print("DB 접속 실패")
except Exception as e:
    print("DB 접속 중 오류가 발생 : ", str(e))

# 원래는 실시간으로 되어있다.



