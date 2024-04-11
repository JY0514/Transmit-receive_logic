import requests
from flask import Flask, request, json, jsonify
import sql
app = Flask(__name__)

sql.dbconnect()

def send_start_api(rider_id, oper_id, start_time, address, request_company):
    API_HOST = "http://127.0.0.1:8091/reception/start"
    url = API_HOST
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    body = {
        "rider_id": rider_id,
        "oper_id": oper_id,
        "start_time": start_time,
        "address": address,
        "request_company": request_company
    }
    requests.post(url, headers=headers, json=body)

def send_end_api(rider_id, oper_id, end_time):
    API_HOST = "http://127.0.0.1:8091/reception/end"
    url = API_HOST
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    body = {
        "rider_id": rider_id,
        "oper_id": oper_id,
        "end_time": end_time
    }
    requests.post(url, headers=headers, json=body)

@app.route("/send", methods=['POST'])
def send_start():
    import sql
    conn = sql.dbconnect()
    cursor = conn.cursor()

    # start와 end 구분
    sql = f"""
               select
                  rider_id,
                  oper_id,
                  DATE_FORMAT(start_time, '%Y-%m-%d %T') as 'time',
                  address as 'address',
                  request_company as 'company',
                  'start' as 'state'
              from
                  logic.s_info
              union all
              select
					    rider_id,
					    oper_id,
					    DATE_FORMAT(end_time, '%Y-%m-%d %T') as 'time',
					    address,
					    request_company as 'company',
					    'end' as 'state'
              from
                  logic.s_info
              order by time asc;
          """
    cursor.execute(sql)
    conn.commit()
    resultu = cursor.fetchall()

    print("API 송신 시작")

    for row in resultu:
        rider_id, oper_id, time, address, company, state = row
        if state == 'start':
            # /reception/start API 호출(운행ID, 기사ID, 시작시간, 주소, 요청사명)
            # json형태로 가공.
            send_start_api(rider_id, oper_id, time, address, company)
            print("운행시작 호출 : " + str(oper_id))
        else:
            # /reception/end API 호출(운행ID, 기사ID, 종료시간)
            # json형태로 가공.
            send_end_api(rider_id, oper_id, time)
            print("운행종료 호출 : " + str(oper_id))

    print("API 송신 완료")

    response = {
        "result": "ok"
    }
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8090)

try:
    connection = sql.dbconnect()
    if connection:
        print("DB 접속 완료")
    else:
        print("DB 접속 실패")
except Exception as e:
    print("DB 접속 중 오류가 발생 : ", str(e))