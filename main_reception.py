from flask import Flask, request, jsonify
import pymysql
from datetime import datetime
import string
import random


app = Flask(__name__)

def dbconnect():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
    return conn

@app.route("/reception/start", methods=['POST'])  # 운행시작
def start():
    params = request.get_json(silent=True)
    print("운행시작")
    oper_ids = params['oper_id']
    rider_ids = params['rider_id']
    start_times = params['start_time']
    addresss = params['address']
    request_companys = params['request_company']
    start_time_s = datetime.strptime(start_times, "%Y-%m-%d %H:%M:%S")

    conn = dbconnect()
    cursor = conn.cursor()
    u_date = datetime.now()

    insert_query = f"""
    INSERT INTO logic.r_info(oper_id, rider_id, start_time, address, request_company, r_date, u_date)
    SELECT '{oper_ids}', '{rider_ids}', '{start_times}', '{addresss}', '{request_companys}', NOW(), '{u_date}' 
    WHERE NOT EXISTS(
        SELECT 1 FROM logic.r_info WHERE rider_id = '{rider_ids}'
    AND oper_id = '{oper_ids}' ); """

    cursor.execute(insert_query)
    conn.commit()

    # 운행시간 업데이트
    # driving_time이 제일 이른 시간이 들어가야함
    start_date_str = start_time_s.strftime("%Y-%m-%d")

    sql_time_check = f"""select rider_id, driving_time from logic.group_all where driving_time = '{start_date_str}' 
                                     """
    cursor.execute(sql_time_check)
    conn.commit()

    sql_rdate = f"""
               select r_date from logic.r_info
                """
    cursor.execute(sql_rdate)
    conn.commit()
    result1 = cursor.fetchall()
    insu_number = 1
    for row in result1:
        r_date = row[0]
        u_date = datetime.now()
        # 1) 라이더 아이디가 중복되지 않게 저장되야함
        insert_query = f""" INSERT INTO logic.group_all (driving_time,start_count, rider_id, end_count, r_date, u_date, group_count)
                            SELECT  '{start_times}', 0,"{rider_ids}", 0, NOW(),' {u_date}', 0
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM logic.group_all
                                   WHERE rider_id = '{rider_ids}'
                                ); """
        cursor.execute(insert_query)
        conn.commit()

        # 보험사 아이디는 처음 start_time입력될때 생성됨
        insu_id = f"TEST{insu_number:05}"
        up_query2 = f""" update logic.r_info set insurance_id = '{insu_id}' where oper_id = %s """
        cursor.execute(up_query2, (oper_ids))
        conn.commit()
        insu_number += 1

        # start_count업데이트
        check_start_count = f""" SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;    """
        cursor.execute(check_start_count)
        conn.commit()
        result_count = cursor.fetchall()
        for row in result_count:
            start_count, rider_id = row
            sqls = f""" update group_all set start_count = '{start_count}' where rider_id = '{rider_id}'; """
            cursor.execute(sqls)
            conn.commit()

    response = {
        "result": "ok"
    }
    return jsonify(response)


@app.route("/reception/end", methods=['POST'])  # 운행종료
def end():
    params = request.get_json(silent=True)
    print("운행종료")

    # 운행 종료 로직 호출
    oper_id11 = params['oper_id']
    rider_id11 = params['rider_id']
    end_time11 = params['end_time']
    conn = dbconnect()
    cursor = conn.cursor()

    # end_count 업데이트 하기전에 이미 업데이트했으면 안하게.. 수정
    # 1. group_all 테이블 end_count + 1
    sql_endcount_up = f"""
    update group_all set end_count = end_count + 1 where rider_id = '{rider_id11}';
    """
    cursor.execute(sql_endcount_up)
    conn.commit()

    # 2. r_info 테이블 end_time UPDATE
    sql_endcount = f""" UPDATE logic.r_info SET end_time = '{end_time11}', d_status = 'complete' WHERE rider_id ='{rider_id11}' and oper_id = '{oper_id11}';  """
    cursor.execute(sql_endcount)
    conn.commit()

    # 3. start_count 와 end_count 동일한 경우
    check = f"""
                          SELECT CASE
                          WHEN start_count = end_count THEN 1
                          ELSE 0
                          END AS 일치여부
                          FROM logic.group_all
                          WHERE rider_id = '{rider_id11}';
                          """
    cursor.execute(check)
    conn.commit()
    result_check = cursor.fetchall()
    if str(result_check) == '((1,),)':
        # group_all 테이블 group_count + 1
        sql_g_count = f"""
            update logic.group_all set group_count = group_count + 1 where rider_id = '{rider_id11}'
            """
        cursor.execute(sql_g_count)
        conn.commit()

        # 그룹아이디 생성
        letters_set = string.ascii_letters
        random_list = random.sample(letters_set, 3)
        result_id = ''.join(random_list)

        group_id = "IDG_"+result_id
        # 생성한 그룹아이디 업데이트
        sqls = f""" UPDATE r_info SET group_id = '{group_id}' WHERE group_id is null AND rider_id = '{rider_id11}' """
        cursor.execute(sqls)
        conn.commit()

        sql_g_time = f"""       select
                                DATE_FORMAT(MIN(start_time), '%Y-%m-%d %T')
                                from logic.r_info
                                where group_id = '{group_id}'
                                """
        cursor.execute(sql_g_time)
        conn.commit()
        result = cursor.fetchone()
        first_start_time = result[0]

        start_time2 = datetime.strptime(first_start_time, "%Y-%m-%d %H:%M:%S")
        end_time2 = datetime.strptime(end_time11, "%Y-%m-%d %H:%M:%S")

        # group_info 생성
        sql_info = f"""
                INSERT IGNORE INTO logic.group_info (group_id, rider_id)
                SELECT %s,%s
                WHERE NOT EXISTS (
                SELECT 1 FROM logic.group_info
                WHERE group_id = '{group_id}' ); """
        cursor.execute(sql_info,
                   (group_id, rider_id11,))
        conn.commit()

        c_operating = end_time2 - start_time2
        # c_operating 분으로 변경
        minutes = c_operating.total_seconds() / 60
        oper_m = int(minutes)

        # c_operating를 분으로
        # 라이더마다 운행시간이 총 300분이 넘는지 아닌지 계산
        sql_rider_oper = f"""
              select SUM(c_operating), SUM(d_amount)
              FROM group_info
              where rider_id = '{rider_id11}'
              and start_time like '{start_time2}%';
        """
        cursor.execute(sql_rider_oper)
        conn.commit()
        result = cursor.fetchone()
        print(result)

        # if(result_min > 300):
        #     d_amount = 0
        # elif(result_min + minutes > 300):
        #     d_amount = 6700 - result_amount
        # else:
        #     total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
        #     d_amount = total_minutes * 19
        #     sql_info_update = f"""
        #     UPDATE logic.group_info
        #     SET start_time = '{first_start_time}', end_time = '{end_time11}',
        #     d_amount = '{d_amount}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
        #     WHERE group_id = '{group_id}'
        #      """
        #     cursor.execute(sql_info_update)
        #     conn.commit()


    response = {
        "result": "ok"
    }
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8091)