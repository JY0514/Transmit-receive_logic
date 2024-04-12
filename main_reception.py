from flask import Flask, request, jsonify
from datetime import datetime
import string
import random
import sql
import pymysql
import math

app = Flask(__name__)


def dbconnect():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
    return conn


@app.route("/reception/start", methods=['POST'])  # 운행시작
def start():
    params = request.get_json(silent=True)
    print("운행시작")
    oper_id = params['oper_id']
    rider_id = params['rider_id']
    start_times = params['start_time']
    address = params['address']
    request_company = params['request_company']

    start_time = datetime.strptime(params['start_time'], "%Y-%m-%d %H:%M:%S")
    start_date = start_time.strftime("%Y-%m-%d")

    conn = dbconnect()
    cursor = conn.cursor()
    
    # r_info 데이터 입력
    sql.insert_r_info(cursor, oper_id, rider_id, start_time, address, request_company, start_times)

    # 보험사 아이디 입력
    sql.generate_insu_id(string, random, cursor, oper_id)

    # 시작시간 입력
    sql.update_start_time(cursor, rider_id, start_date)

    conn.commit()
    response = {"result": "ok"}
    return jsonify(response)

@app.route("/reception/end", methods=['POST'])  # 운행종료
def end():
    params = request.get_json(silent=True)
    print("운행종료")  # 운행 종료 로직 호출
    oper_id = params['oper_id']
    rider_id = params['rider_id']
    end_time = params['end_time']
    global group_min_start_time

    conn = dbconnect()
    cursor = conn.cursor()

    sql_r_info = f"""
        select start_time
          from r_info
          where oper_id = '{oper_id}';
    """
    cursor.execute(sql_r_info)

    start_datetime = cursor.fetchone()
    end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    # Date(YYYY-MM-DD 형식 문자열포맷팅)
    start_time_str = start_datetime[0].strftime("%Y-%m-%d")

    # 그룹(다건)운행 중인지 아닌지에 대한 체크
    sql.check_time(rider_id, cursor)
    min_start_time = cursor.fetchone()

    # 1. 카운트 올리기--------------------------------------------------------------------------
    if min_start_time[0] is not None:
        group_driving_time = min_start_time[0].strftime("%Y-%m-%d")
        sql.update_end_count(rider_id, group_driving_time, cursor)
        conn.commit()

        group_min_start_time = group_driving_time

    else:
        sql.update_end_count(rider_id, start_time_str, cursor)
        conn.commit()

        group_min_start_time = start_time_str

    # 2. r_info end_time Update --------------------------------------------------------------------------
    sql.endtime_update(end_datetime, oper_id, cursor)
    conn.commit()

    # 3. group_all start/end count 추출  --------------------------------------------------------------------------
    sql.count_groupall(rider_id, group_min_start_time, cursor)
    count_result = cursor.fetchone()

    start_count = count_result[0]
    end_count = count_result[1]

    # 4. 그룹(다건)운행 종료 구분
    if start_count == end_count:
        # 5. 그룹 카운트 올리기 --------------------------------------------------------------------------
        sql.update_group_count(rider_id, group_min_start_time, cursor)
        conn.commit()

        # 6. 그룹(다건)운행 중 가장 빠른 시작시간 추출  --------------------------------------------------------------------------
        sql.first_time(rider_id, cursor)
        first_start_time_result = cursor.fetchone()
        first_start_time = datetime.strptime(first_start_time_result[0], "%Y-%m-%d %H:%M:%S")

        # 7. 그룹 ID 채번  --------------------------------------------------------------------------
        letters_set = string.ascii_letters
        group_date = first_start_time.strftime("%Y%m%d")
        group_id = "IDG" + group_date + "-" + rider_id + "-" + ''.join(random.sample(letters_set, 3))

        # 8. 그룹 ID Update start --------------------------------------------------------------------------
        sql.groupid_update(group_id, rider_id, cursor)
        conn.commit()
        # 8. 그룹 ID Update end --------------------------------------------------------------------------
        end_time_s = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        start_times = start_datetime[0]
        if first_start_time is not None:

            # c_operating 분으로 변경 및 보험료 구하는 계산
            c_operating = end_time_s - start_times
            minutes = c_operating.total_seconds() / 60
            oper_m = math.ceil(minutes)
            total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
            d_amount = total_minutes * 19  # 건마다 들어가는 보험료

            # 보험료 산출
            d_amount2 = sql.d_amounts(sql, rider_id,cursor, d_amount, conn, oper_m, group_id, first_start_time, end_time_s)
            print(f"보험료: {d_amount2}")

            #  group_info 데이터 insert
            sql.insert_group_info(group_id, cursor, rider_id,first_start_time ,end_time_s,d_amount2,oper_m)

            conn.commit()

    response = {
        "result": "ok"
    }
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8091)