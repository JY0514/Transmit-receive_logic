from flask import Flask, request, jsonify
import pymysql
from datetime import datetime

app = Flask(__name__)

def dbconnect():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
    return conn


@app.route("/reception/start", methods=['POST'])  # 운행시작
def start():
    params = request.get_json()

    print("운행시작")

    oper_ids = params['oper_id']
    rider_ids = params['rider_id']
    start_times = params['start_time']
    addresss = params['address']
    request_companys = params['request_company']

    conn = dbconnect()
    cursor = conn.cursor()

    u_date = datetime.now()
    insert_query = f"""
        INSERT INTO logic.r_info (oper_id, rider_id, start_time, address, request_company, r_date, u_date)
        values( '{params['oper_id']}', '{params['rider_id']}', '{params['start_time']}', '{params['address']}', '{params['request_company']}', '{params['start_time']}', '{u_date}');
     
    """

    # INSERT
    # INTO
    # logic.r_info(oper_id, rider_id, start_time, address, request_company, r_date, u_date)
    # SELECT
    # '{oper_ids}', '{rider_ids}', '{start_times}', '{addresss}', '{request_companys}', '{start_times}', '{u_date}'
    # WHERE
    # NOT
    # EXISTS(
    #     SELECT
    # 1
    # FROM
    # logic.r_info
    # WHERE
    # rider_id = '{rider_ids}'
    # AND
    # oper_id = '{oper_ids}'
    # );
    try:
        cursor.execute(insert_query)
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print("Error during insertion:", e)


    # 운행시간 업데이트
    # driving_time이 제일 이른 시간이 들어가야함
    start_time_s = datetime.strptime(start_times, "%Y-%m-%d %H:%M:%S")

    # Extract date part using strftime
    start_date_str = start_time_s.strftime("%Y-%m-%d")

    sql_time_check = f"""
                                     select rider_id, driving_time from logic.group_all where driving_time = '{start_date_str}' 
                                     """
    cursor.execute(sql_time_check)
    conn.commit()
    resultsss = cursor.fetchall()
    if len(resultsss) == 0:
        sql_rdate = f"""
                select r_date from group_info
                """
        cursor.execute(sql_rdate)
        conn.commit()
        result1 = cursor.fetchall()
        for row in result1:
            r_date = row[0]
            u_date = datetime.now()
                    # 1) 라이더 아이디가 중복되지 않게 저장되야함
            insert_query = f"""
                                                         INSERT INTO logic.group_all (driving_time,start_count, rider_id, end_count, r_date, u_date, group_count)
                                                         SELECT  '{start_times}', 0,'{rider_ids}', 0, '{r_date}',' {u_date}', 0
                                                         WHERE NOT EXISTS (
                                                             SELECT 1 FROM logic.group_all
                                                            WHERE rider_id = '{rider_ids}'
                                                         );
                                                           """
            cursor.execute(insert_query)
            conn.commit()

        check_start_count = f"""
                               SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;"""
        cursor.execute(check_start_count)
        conn.commit()
        result_count = cursor.fetchall()
        for row in result_count:
            start_count, rider_id = row
            sqls = f"""
                    update group_all set start_count = '{start_count}' where rider_id = '{rider_id}'   
                    """
            cursor.execute(sqls)
            conn.commit()

    else:
        # #2)start_count는 group_info에 있는 데이터 갯수만큼 정해지는 것임
        check_start_count = f"""
                                              SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;"""
        cursor.execute(check_start_count)
        conn.commit()
        result_count = cursor.fetchall()
        for row in result_count:
            start_count, rider_id = row
            sqls = f"""
                                   update group_all set start_count = '{start_count}' where rider_id = '{rider_id}'
                                   """
            cursor.execute(sqls)
            conn.commit()

    response = {
        "result": "ok"
    }
    return jsonify(response)


@app.route("/reception/end", methods=['POST'])  # 운행종료
def end():
    params = request.get_json()

    print("운행종료")
    
    # 운행 종료 로직 호출
    oper_id = params['oper_id']
    print(oper_id)
    rider_id = params['rider_id']
    print(rider_id)
    end_time = params['end_time']
    print(end_time)

    conn = dbconnect()
    cursor = conn.cursor()

    sql_endcount = f"""
                             UPDATE logic.group_all g
                             INNER JOIN (
                               SELECT rider_id, COUNT(*) as cnt
                               FROM logic.group_info
                               GROUP BY rider_id
                             ) i ON g.rider_id = i.rider_id
                             SET g.end_count = i.cnt
                             WHERE g.rider_id = '{rider_id}';
                                   """

    cursor.execute(sql_endcount)
    conn.commit()
    sql_end_check = f"""
    SELECT  COUNT(end_time) AS end_count, rider_id  FROM logic.r_info GROUP BY rider_id;
    """
    cursor.execute(sql_end_check)
    conn.commit()
    result_count = cursor.fetchall()
    for row in result_count:
        end_count, rider_id = row
        sql_allupdate = f"""
                          UPDATE logic.group_all SET end_count = '{end_count}' WHERE rider_id ='{rider_id}'
                                                      """

        cursor.execute(sql_allupdate)
        conn.commit()

        check = f"""
            SELECT CASE
            WHEN start_count = end_count THEN 1
            ELSE 0
            END AS 일치여부
            FROM logic.group_all
            WHERE rider_id = '{rider_id}';
            """
        cursor.execute(check)
        conn.commit()
        resultssss = cursor.fetchall()

        if resultssss == '1':
            print("이미 업데이트 됨")
        else:
            # group_count가 end_count 갯수와 일치하지 않나...싶은데
            up_sql = f"""
            UPDATE logic.group_all
            SET group_count = end_count
            WHERE rider_id = '{rider_id}';
                        """
            cursor.execute(up_sql)
            conn.commit()

            # group_count +1
            # sql_update = f"""
            #                     UPDATE logic.group_all SET group_count = 1 WHERE rider_id ='{rider_id}'
            #                                         """
            # cursor.execute(sql_update)
            # conn.commit()

    #  s_info에 데이터 조회 및 end_time 업데이트
    sql_check = f"""select  oper_id, end_time  from logic.s_info"""
    cursor.execute(sql_check)
    conn.commit()
    result = cursor.fetchall()

    insu_number = 1
    for row in result:
        oper_ids, end_times = row

        # end_times 값이 존재할 때만 UPDATE 구문 실행
        if end_times is not None:
            sql_r_info_up = f"""
                                        UPDATE logic.r_info SET end_time = '{end_times}', u_date = NOW() WHERE oper_id ='{oper_ids}';
                                    """
            cursor.execute(sql_r_info_up)
            conn.commit()

            # 보험사 아이디 업데이트
            insu_id = f"TEST{insu_number:05}"
            up_query2 = f"""
                                  update logic.r_info set insurance_id = '{insu_id}' where oper_id = %s
                                  """
            cursor.execute(up_query2, (oper_ids))
            conn.commit()
            insu_number += 1

    # 5.다건 배송이 종료될때 그룹 아이디 생성
    # group id는 종료 시간까지 다 나왔을때 입력해줘야한다. (그래서 state == end일때 구한다.)
    # group_id 조회(같은 라이더 중에서 배달 시간이 겹치는 부분이 있으면 group_id로 묶는다)
    sql_group_id = f"""
                                 SELECT rider_id, end_time ,CONCAT('callID_',a.rider_id, a.oper_id) AS group_id, start_time,oper_id
                                 FROM logic.s_info a
                                 WHERE EXISTS (
                                     SELECT 1
                                     FROM r_info b
                                     WHERE a.rider_id != b.rider_id
                                     AND a.start_time < b.end_time
                                     AND b.start_time < a.end_time
                                 );
                                 """
    cursor.execute(sql_group_id)
    conn.commit()
    results = cursor.fetchall()

    for row in results:
        rider_id, end_time, group_id, start_time, oper_id = row
        sql_update_groupid = f"""
                                UPDATE logic.r_info SET group_id = %s WHERE rider_id = %s and end_time = %s and start_time = %s
                                """
        cursor.execute(sql_update_groupid, (group_id, rider_id, end_time, start_time))
        conn.commit()

        # insu_number 변수를 문자열로 변환
        # d_status 운행상태 업데이트
        up_query = f"""
                                            UPDATE logic.r_info
                                               SET d_status = CASE
                                                    WHEN end_time IS NULL THEN 'driving'
                                                                           ELSE 'complete'
                                                                           END
    
                                               WHERE rider_id = %s """
        cursor.execute(up_query, (rider_id))
        conn.commit()

    # 7. d_status == complete 일 경우에만 group_info해준다 (배달 완료된 건에 대해서만) / 운행시간 및 보험료 계산
    sql = f"""
                            select group_id, rider_id, start_time, end_time, oper_id from logic.r_info
                            """
    cursor.execute(sql)
    conn.commit()
    result = cursor.fetchall()

    for row in result:  # null인 값이 없어야함
        group_id, rider_id, start_time, end_time, r_date = row
        if row[0] is not None:
            c_operating = end_time - start_time
            total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
            d_amount = total_minutes * 19
            u_date = datetime.now()

            # 고쳐야하는 부분
            # 보험사 아이디, 배달 상태, group_all

            sql_ginfo = f"""
                             select r_date  from r_info
                                    """
            cursor.execute(sql_ginfo)
            conn.commit()
            resultinfo = cursor.fetchall()
            r_dates = resultinfo[0]

            # group_info 테이블 데이터 삽입
            sql_info = f"""
                                                      INSERT IGNORE INTO logic.group_info (c_operating, d_amount,group_id, rider_id, start_time, end_time, r_date, u_date)
                                                      SELECT %s,%s, %s, %s, %s, %s, %s, %s
                                                      WHERE NOT EXISTS (
                                                          SELECT 1 FROM logic.group_info
                                                          WHERE group_id = %s
                                                      );
                                                      """
            cursor.execute(sql_info, (
                c_operating, d_amount, group_id, rider_id, start_time, end_time, r_dates, u_date, group_id))
            conn.commit()

    response = {
        "result": "ok"
    }
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8091)
