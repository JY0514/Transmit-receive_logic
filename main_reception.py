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
    oper_id = params['oper_id']
    rider_id = params['rider_id']
    start_time = params['start_time']
    address = params['address']
    request_company = params['request_company']
    start_time_s = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    conn = dbconnect()
    cursor = conn.cursor()
    u_date = datetime.now()

    insert_query = f"""
    INSERT INTO logic.r_info(oper_id, rider_id, start_time, address, request_company, r_date, u_date)
    SELECT '{oper_id}', '{rider_id}', '{start_time}', '{address}', '{request_company}', NOW(), '{u_date}' 
    WHERE NOT EXISTS(
        SELECT 1 FROM logic.r_info WHERE rider_id = '{rider_id}'
    AND oper_id = '{oper_id}' ); """

    cursor.execute(insert_query)
    conn.commit()

    # 운행시간 업데이트
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
        u_date = datetime.now()
        #  라이더 아이디가 중복되지 않게 저장되야함
        insert_query = f""" INSERT INTO logic.group_all (driving_time, start_count, rider_id, end_count, r_date, u_date, group_count)
        SELECT '{start_time}', 0, '{rider_id}', 0, NOW(), '{u_date}', 0
        WHERE NOT EXISTS (
            SELECT 1 FROM logic.group_all
            WHERE rider_id = '{rider_id}' AND DATE(driving_time) = DATE('{start_time}')
        );
"""
        cursor.execute(insert_query)
        conn.commit()

        # 보험사 아이디는 처음 start_time 입력될때 생성
        insu_id = f"TEST{insu_number:05}"
        up_query2 = f""" update logic.r_info set insurance_id = '{insu_id}' where oper_id = %s """
        cursor.execute(up_query2, (oper_id))
        conn.commit()
        insu_number += 1

    sql_startcount_up = f"""
                    update group_all set start_count = start_count + 1 where rider_id = '{rider_id}' and driving_time like '{start_date_str}%';
                    """
    cursor.execute(sql_startcount_up)
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
    oper_id = params['oper_id']
    rider_id = params['rider_id']
    end_time = params['end_time']
    conn = dbconnect()
    cursor = conn.cursor()

    # #  group_all 테이블 end_count + 1
    # end_time format..
    end_time_s = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    sql = f"""
        select start_time
          from r_info
          where oper_id = '{oper_id}';
    """
    cursor.execute(sql)
    conn.commit()
    end_time2 = cursor.fetchone()
    end_time_s = end_time2[0]
    end_date_str = end_time_s.strftime("%Y-%m-%d")
    sql_endcount_up = f"""
     update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{end_date_str}%';
             """
    cursor.execute(sql_endcount_up)
    conn.commit()

    # sql = f"""
    #       select start_time,rider_id  from r_info
    # """
    # cursor.execute(sql)
    # conn.commit()

    #  r_info 테이블 end_time UPDATE
    sql_endcount = f""" UPDATE logic.r_info SET end_time = '{end_time}', d_status = 'complete' WHERE rider_id ='{rider_id}' and oper_id = '{oper_id}';  """
    cursor.execute(sql_endcount)
    conn.commit()

    #  start_count 와 end_count 동일한지 확인
    check = f"""
                SELECT CASE
                WHEN start_count = end_count THEN 1
                ELSE 0
                END AS 일치여부
                FROM logic.group_all
                WHERE rider_id = '{rider_id}' and driving_time = '{end_date_str}';
                          """
    cursor.execute(check)
    conn.commit()
    result_check = cursor.fetchall()
    print(rider_id)
    print(str(result_check))
    if str(result_check) == '((1,),)':  # 일치한 경우 1
        # group_all 테이블 group_count + 1
        sql_g_count = f"""
            update logic.group_all set group_count = group_count + 1 where rider_id = '{rider_id}'
            """
        cursor.execute(sql_g_count)
        conn.commit()

        # 그룹아이디 생성
        letters_set = string.ascii_letters
        random_list = random.sample(letters_set, 3)
        result_id = ''.join(random_list)

        group_id = "IDG_" + result_id
        # 생성한 그룹아이디 업데이트
        sqls = f""" UPDATE r_info SET group_id = '{group_id}' WHERE group_id is null AND rider_id = '{rider_id}' """
        cursor.execute(sqls)
        conn.commit()

        # 이상하게 r_info에 존재하지 않는 그룹아이디가 들어가서 체크하고.. 생성함
        sql_check_group = f"""
            SELECT COUNT(*)
            FROM logic.group_info
            WHERE group_id = '{group_id}';
        """
        cursor.execute(sql_check_group)
        conn.commit()
        result_group_check = cursor.fetchone()[0]
        if result_group_check == 0:
            # group_info 생성
            sql_info = f"""
                    INSERT IGNORE INTO logic.group_info (group_id, rider_id)
                    SELECT %s,%s
                    WHERE NOT EXISTS (
                    SELECT 1 FROM logic.group_info
                    WHERE group_id = '{group_id}' ); """

            cursor.execute(sql_info,(group_id, rider_id))
            conn.commit()

            # start_time 제일 이른 시간
            sql_g_time = f"""       select
                                    DATE_FORMAT(MIN(start_time), '%Y-%m-%d %T')
                                    from logic.r_info
                                    where group_id = '{group_id}'
                                    """
            cursor.execute(sql_g_time)
            conn.commit()
            result = cursor.fetchone()
            first_start_time = result[0]

            end_time_f = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            # time format
            if first_start_time is not None:
                start_time_f = datetime.strptime(first_start_time, "%Y-%m-%d %H:%M:%S")
                # c_operating 분으로 변경 및 보험료 구하는 계산
                c_operating = end_time_f - start_time_f
                minutes = c_operating.total_seconds() / 60
                oper_m = int(minutes)  # 건마다 들어가는 운행시간
                total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
                d_amount = total_minutes * 19  # 건마다 들어가는 보험료

                # 여기서 업데이트해야 밑에 쿼리문이 돌아감
                sql_info_update = f"""
                                  UPDATE logic.group_info
                                  SET start_time = '{first_start_time}', end_time = '{end_time}',
                                  d_amount = '{d_amount}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
                                  WHERE group_id = '{group_id}'
                                   """
                cursor.execute(sql_info_update)
                conn.commit()

                # 라이더마다 운행시간이 총 300분인지 확인
                sql_rider_oper = f"""
                            select SUM(c_operating), SUM(d_amount)
                            FROM group_info
                            where rider_id = '{rider_id}';
                      """
                cursor.execute(sql_rider_oper)
                conn.commit()
                result = cursor.fetchone()

                oper_time = int(result[0]) - oper_m  # #현재 추가되는 운행 제외하고 기존에 있던 시간
                d_amount_now = int(result[1]) - d_amount  # 현재 추가되는 운행 제외하고 기존에 있던 보험료
                time = result[0] - oper_time  # 현재 업데이트하려는 시간

                if oper_time > 300:  # 현재 운행 시간이 300이 넘는지
                    print("현재 운행 시간이 300분 초과")
                    # print(oper_time)
                    # start_time이 다르면 제외함
                    d_amounts = 0
                    sql_info_update = f"""
                                                             UPDATE logic.group_info
                                                             SET start_time = '{first_start_time}', end_time = '{end_time_f}',
                                                             d_amount = '{d_amounts}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
                                                             WHERE group_id = '{group_id}' and DATE(start_time) != DATE('{end_date_str}')
                                                              """
                    cursor.execute(sql_info_update)
                    conn.commit()

                elif time + oper_time > 300:  # 지금 추가되는 시간이 300이 넘는지
                    # 새로 추가되는 시간이 더해져야함
                    print("지금까지의 운행 시간에 현재 운행 시간이 더해져서 300분이 초과")
                    d_amounts = 6700 - d_amount_now
                    sql_info_update = f"""
                                               UPDATE logic.group_info
                                               SET start_time = '{first_start_time}', end_time = '{end_time_f}',
                                               d_amount = '{d_amounts}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
                                               WHERE group_id = '{group_id}'
                                                """
                    cursor.execute(sql_info_update)
                    conn.commit()
                elif time < 300:  # 지금 추가되는 시간이 300이 넘는지

                    sql_info_update = f"""
                                                       UPDATE logic.group_info
                                                       SET start_time = '{first_start_time}', end_time = '{end_time_f}',
                                                       d_amount = '{d_amount}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
                                                       WHERE group_id = '{group_id}'
                                                        """
                    cursor.execute(sql_info_update)
                    conn.commit()

                else:  # 운행시간이 300이 안넘음
                    print("운행 시간이 300분이 초과X")
                    sql_info_update = f"""
                          UPDATE logic.group_info
                          SET start_time = '{first_start_time}', end_time = '{end_time_f}',
                          d_amount = '{d_amount}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
                          WHERE group_id = '{group_id}' 
                           """
                    cursor.execute(sql_info_update)
                    conn.commit()
            else:
                print("으악")

    response = {
        "result": "ok"
    }
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8091)
