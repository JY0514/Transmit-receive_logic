from flask import Flask, request, jsonify
from datetime import datetime
import string
import random
import sql
import database

app = Flask(__name__)

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

    conn = database.dbconnect()
    cursor = conn.cursor()

    sql.insert_r_info(cursor, oper_id, rider_id, start_time,address,request_company,start_times)
    sql.generate_insu_id(string, random, cursor, oper_id)
    sql.update_start_time(cursor,rider_id, start_date)

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
    conn = database.dbconnect()
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
    global group_min_start_time

    # 그룹(다건)운행 중인지 아닌지에 대한 체크
    sql_min_start_time = f""" SELECT MIN(start_time) FROM r_info WHERE group_id is NULL AND rider_id = '{rider_id}'; """
    cursor.execute(sql_min_start_time)
    min_start_time = cursor.fetchone()

    # 1. 카운트 올리기 start --------------------------------------------------------------------------
    if min_start_time[0] is not None:
        group_driving_time = min_start_time[0].strftime("%Y-%m-%d")

        sql_update_endcount = f"""
        update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{group_driving_time}%';
                """
        cursor.execute(sql_update_endcount)
        conn.commit()

        group_min_start_time = group_driving_time
    else:
        sql_update_endcount = f"""
        update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{start_time_str}%';
                """
        cursor.execute(sql_update_endcount)
        conn.commit()

        group_min_start_time = start_time_str
    # 1. 카운트 올리기 end --------------------------------------------------------------------------

    # 2. r_info end_time Update start --------------------------------------------------------------------------
    sql_update_endtime = f""" UPDATE r_info SET end_time = '{end_datetime}', d_status = 'complete', u_date = NOW() WHERE  oper_id = '{oper_id}';  """
    cursor.execute(sql_update_endtime)
    conn.commit()
    # 2. r_info end_time Update start --------------------------------------------------------------------------

    # 3. group_all start/end count 추출 start --------------------------------------------------------------------------
    sql_count_result = f""" SELECT start_count, end_count from group_all WHERE rider_id = '{rider_id}' and driving_time like '{group_min_start_time}';  """
    cursor.execute(sql_count_result)
    count_result = cursor.fetchone()

    start_count = count_result[0]
    end_count = count_result[1]
    # 3. group_all start/end count 추출 end --------------------------------------------------------------------------

    # 4. 그룹(다건)운행 종료 구분
    if (start_count == end_count):

        # 5. 그룹 카운트 올리기 start --------------------------------------------------------------------------
        sql_update_group_count = f""" UPDATE group_all SET group_count = group_count + 1 WHERE rider_id = '{rider_id}' and driving_time like '{group_min_start_time}'; """
        cursor.execute(sql_update_group_count)
        conn.commit()
        # 5. 그룹 카운트 올리기 end --------------------------------------------------------------------------

        # 6. 그룹(다건)운행 중 가장 빠른 시작시간 추출 start --------------------------------------------------------------------------
        sql_first_start_time_result = f""" select DATE_FORMAT(MIN(start_time), '%Y-%m-%d %T') from r_info WHERE group_id is null AND rider_id = '{rider_id}'; """
        cursor.execute(sql_first_start_time_result)
        first_start_time_result = cursor.fetchone()

        first_start_time = datetime.strptime(first_start_time_result[0], "%Y-%m-%d %H:%M:%S")
        # 6. 그룹(다건)운행 중 가장 빠른 시작시간 추출 end --------------------------------------------------------------------------

        # 7. 그룹 ID 채번 start --------------------------------------------------------------------------
        letters_set = string.ascii_letters
        group_date = first_start_time.strftime("%Y%m%d")
        group_id = "IDG" + group_date + "-" + rider_id + "-" + ''.join(random.sample(letters_set, 3))

        # 7. 그룹 ID 채번 end --------------------------------------------------------------------------

        # 8. 그룹 ID Update start --------------------------------------------------------------------------
        sql_update_group_id = f""" UPDATE r_info SET group_id = '{group_id}' WHERE group_id is null AND rider_id = '{rider_id}' """
        cursor.execute(sql_update_group_id)
        conn.commit()
        # 8. 그룹 ID Update end --------------------------------------------------------------------------
        end_time_s = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        start_times = start_datetime[0]
        if first_start_time is not None:
            # c_operating 분으로 변경 및 보험료 구하는 계산
            c_operating = end_time_s - start_times
            minutes = c_operating.total_seconds() / 60
            oper_m = int(minutes)
            total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
            d_amount = total_minutes * 19  # 건마다 들어가는 보험료
            sql_info = f"""
                                    INSERT IGNORE INTO logic.group_info (group_id, rider_id)
                                    SELECT %s,%s
                                    WHERE NOT EXISTS (
                                    SELECT 1 FROM logic.group_info
                                    WHERE group_id = '{group_id}' ); """
            cursor.execute(sql_info, (group_id, rider_id))
            conn.commit()
            # 여기서 업데이트해야 밑에 쿼리문이 돌아감
            sql.info_update(first_start_time, end_time_s, d_amount, oper_m, group_id, cursor)
            conn.commit()

            # 라이더마다 운행시간, 운행시작시간, 총 보험료 확인
            sql_rider_oper = f"""
            SELECT DATE(start_time), SUM(c_operating), SUM(d_amount)
            FROM group_info
            WHERE rider_id = '{rider_id}'
            GROUP BY DATE(start_time)
            ORDER BY DATE(start_time);"""
            cursor.execute(sql_rider_oper)
            conn.commit()
            result = cursor.fetchall()
            for row in result:
                date, oper, d = row
                oper_time = oper - oper_m  # 현재 추가되는 운행 제외하고 기존에 있던 시간
                d_amount_now = d - d_amount  # 현재 추가되는 운행 제외하고 기존에 있던 보험료
                time = oper - oper_time  # 현재 업데이트하려는 시간
                for date in row:
                    if oper_time > 300:  # 현재 누적 운행 시간이 300이 넘을 때
                        print("현재 누적 운행 시간이 300분 초과")
                        sql_date_check = f"""
                                        SELECT DATE(start_time), DATE(end_time)
                                        FROM group_info
                                        WHERE group_id = '{group_id}'
                                    """
                        cursor.execute(sql_date_check)
                        conn.commit()
                        date_result = cursor.fetchall()
                        if len(date_result) != 1 or date_result[0][0] != date_result[0][1]:  # start_time과 end_time 날짜가 다른 경우
                            d_amounts = 0
                            sql.info_update(first_start_time, end_time_s, d_amounts, oper_m, group_id, cursor)
                            conn.commit()

                        else:  # start_time과 end_time 날짜가 같은 경우
                            d_amounts = 0
                            sql.info_update(first_start_time, end_time_s, d_amounts, oper_m, group_id, cursor)
                            conn.commit()

                    elif time + oper_time > 300:  # 지금 추가되는 시간이 300이 넘는지
                        # 새로 추가되는 시간이 더해져야함
                        print("지금까지의 운행 시간에 현재 운행 시간이 더해져서 300분이 초과")
                        d_amounts = 6700 - d_amount_now
                        sql.info_update(first_start_time, end_time_s, d_amounts, oper_m, group_id, cursor)
                        conn.commit()

                    elif time < 300:  # 지금 추가되는 시간이 300이 넘는지 d_amount
                        sql.info_update(first_start_time, end_time_s, d_amount, oper_m, group_id, cursor)
                        conn.commit()

                    else:  # 운행시간이 300이 안넘음
                        print("운행 시간이 300분이 초과X")
                        sql.info_update(first_start_time, end_time_s, d_amounts, oper_m, group_id, cursor)
                        conn.commit()

                else:
                    print("")
    response = {
        "result": "ok"
    }
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8091)
