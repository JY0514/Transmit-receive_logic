import pymysql
from datetime import datetime
import schedule
import time

def dbconnect():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
    return conn

try:
    connection = dbconnect()
    if connection:
        print("DB 접속 완료")
    else:
        print("DB 접속 실패")
except Exception as e:
    print("DB 접속 중 오류가 발생 : ", str(e))

def send():   #송신 로직
    conn = dbconnect()
    cursor = conn.cursor()

    # 송신 첫번째
    sql_send = f"""
    INSERT INTO logic.s_info (oper_id, rider_id ,  start_time ,   address ,request_company)
    VALUES 
    ('1','a', '2024-03-27 11:20',  'abc', 'abc'),
    ('2','b',  '2024-03-27 12:24',  'abc', 'abc'),
    ('3','a',  '2024-03-27 13:26', 'abc', 'abc'),
    ('4','c',  '2024-03-27 14:06',  'abc', 'abc'),
    ('5','b',  '2024-03-27 11:27', 'abc', 'abc'),
    ('6','c',  '2024-03-27 12:06',  'abc', 'abc'),
    ('7','b',  '2024-03-27 13:37',  'abc', 'abc'),
    ('8','b',  '2024-03-27 14:32',  'abc', 'abc'),
    ('9','b',  '2024-03-27 16:01', 'abc', 'abc'),
    ('10','a',  '2024-03-27 15:20',  'abc', 'abc')
    """
    cursor.execute(sql_send)
    conn.commit()

    cursor.execute("SELECT  rider_id, end_time FROM logic.s_info")
    result = cursor.fetchall()

    for i, row in enumerate(result):
        end_time_values = [
            '2024-03-27 11:30', '2024-03-27 11:30', '2024-03-27 12:34', '2024-03-27 13:30', '2024-03-27 14:26',
            '2024-03-27 11:29', '2024-03-27 12:46', '2024-03-27 14:37', '2024-03-27 16:21', '2024-03-27 15:25'
        ]
        rider_id, end_time = row
        # end_time_values 리스트에서 해당 인덱스의 값을 선택
        end_time_value = end_time_values[i]

        # 송신 end_time 들어가는 부분
        sql_end = """
        UPDATE logic.s_info SET end_time = %s WHERE rider_id = %s
        """
        cursor.execute(sql_end, (end_time_value, rider_id))
        conn.commit()

# 수신 시작 로직
def reception():
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logic.s_info")
    result = cursor.fetchall()
    data_count = len(result)
    print(f"데이터 갯수: {data_count}")

    # 1. 송신
    sql = f"""
        select
            rider_id,
            start_time as 'time',
            'start' as 'state'
        from
            logic.s_info
        union all
        select
            rider_id,
            end_time as 'time',
            'end' as 'state'
        from
            logic.s_info
        order by time asc;
    """
    cursor.execute(sql)
    conn.commit()
    resultu = cursor.fetchall()

    for row in resultu:
        rider_id, time, state = row
        if state == 'start':
            print('수신 운행시작')
            #수신(운행시작)
            sql_check = f"""select   *  from s_info"""
            cursor.execute(sql_check)
            conn.commit()
            result = cursor.fetchall()
            for record in result:
                rider_id, oper_id, start_time, end_time, address, request_company = record
                print(record)
                now = datetime.now()
                r_date = now.strftime('%Y-%m-%d %H:%M:%S')
                insu_number = 1
                insu_id = f"TEST{insu_number:05}"
                end_time_value = end_time if end_time is not None else '0000-00-00 00:00:00'


# sql_group_id = "   SELECT CONCAT('group', '{rider_id}') AS group_id, "

                insert_query = f"""
                           INSERT INTO logic.r_info (group_id, oper_id, rider_id, start_time, end_time, address, request_company, d_status, insurance_id, r_date, u_date)
                            SELECT CONCAT('group', '{rider_id}') AS group_id, '{oper_id}','{rider_id}', '{start_time}', '{end_time_value}', '{address}', '{request_company}',
                            CASE WHEN '{end_time_value}' != '0000-00-00 00:00:00' THEN 'complete' ELSE 'driving' END, '{insu_id}', '{r_date}', '{r_date}'
                            WHERE NOT EXISTS (
                                SELECT 1 FROM logic.r_info
                                WHERE rider_id = '{rider_id}' AND oper_id = '{oper_id}'
                            );
                        """
                cursor.execute(insert_query)
                conn.commit()
                insu_number += 1
        # else:
        #     print('else')
            #     수신(운행종료, 그룹핑)

            # rider_id와 group_id별로 최소 start_time과 최대 end_time 가져온다
            # sql2 = """
            #    SELECT rider_id, group_id, MIN(start_time) AS earliest_start_time, MAX(end_time) AS latest_end_time
            #    FROM logic.r_info
            #    GROUP BY rider_id, group_id;
            #    """
            # cursor.execute(sql2)
            # result2 = cursor.fetchall()
            #
            # for row in result2:
            #     rider_id, group_id, start_time, end_time = row
            #     cursor.execute("SELECT TIMESTAMPDIFF(MINUTE, %s, %s)", (start_time, end_time))
            #     c_operating_minutess = cursor.fetchone()[0]
            #     c_operating_minutes = c_operating_minutess if c_operating_minutess is not None else 0
            #     c_operating = end_time - start_time
            #     time_difference_str = str(c_operating)
            #
            #     # 분당 19원으로 계산
            #     d_amount = c_operating_minutes * 19
            #
            #     # group_info 테이블 데이터 삽입
            #     sql3 = """
            #        INSERT INTO logic.group_info (c_operating, group_id, rider_id, start_time, end_time, r_date, u_date)
            #        SELECT %s, %s, %s, %s, %s, %s, %s
            #        WHERE NOT EXISTS (
            #            SELECT 1 FROM logic.group_info
            #            WHERE group_id = %s AND rider_id = %s
            #        );
            #        """
            #     cursor.execute(sql3,
            #                    (time_difference_str, group_id, rider_id, start_time, end_time, r_date, r_date, group_id,
            #                     rider_id))
            #     conn.commit()
            #
            #     # group_info 총 운행 시간에 대한 가격 업데이트
            #     update_query = "UPDATE logic.group_info SET d_amount = %s WHERE rider_id = %s AND group_id = %s"
            #     cursor.execute(update_query, (d_amount, rider_id, group_id))
            #     conn.commit()
            #
            #     # group_all 테이블에 데이터 삽입
            #     sql33 = """
            #        INSERT INTO logic.group_all (driving_time, rider_id, r_date, u_date, group_count)
            #        SELECT %s, %s, %s, %s, 0
            #        WHERE NOT EXISTS (
            #            SELECT 1 FROM logic.group_all
            #            WHERE rider_id = %s
            #        );
            #        """
            #     cursor.execute(sql33, (time_difference_str, rider_id, r_date, r_date, rider_id))
            #     conn.commit()
            #
            #     # 기본적으로 end_count 0 입력
            #     ql2_update = """
            #        UPDATE group_all
            #        SET end_count = 0
            #        WHERE rider_id = %s
            #        """
            #     cursor.execute(ql2_update, (rider_id,))
            #     conn.commit()
            #
            #     # start_count 데이터 입력
            #     sql = f"SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;"
            #     cursor.execute(sql)
            #     conn.commit()
            #     result_d = cursor.fetchall()
            #
            #     for tuple in result_d:
            #         start_count = tuple[0]
            #         rider_ids = tuple[1]
            #         # start_time이 몇개 인지 확인한 결과를 group_all 테이블 업데이트
            #         sql2_update = f"""
            #                                 update group_all
            #                                  set start_count  = '{start_count}'
            #                                  where group_all.rider_id = '{rider_ids}'
            #                              """
            #         cursor.execute(sql2_update)
            #         conn.commit()
            #
            #     # end_count 내용 업데이트
            #     ql3_update = """
            #        SELECT COUNT(*), rider_id
            #        FROM r_info
            #        WHERE end_time
            #        GROUP BY rider_id;
            #        """
            #     cursor.execute(ql3_update)
            #     resultu = cursor.fetchall()
            #
            #     for row in resultu:
            #         end_count, rider_idss = row
            #         ql4_update = """
            #            UPDATE group_all SET end_count = %s WHERE rider_id = %s
            #            """
            #         cursor.execute(ql4_update, (end_count, rider_idss))
            #         conn.commit()
            #
            #         # group_all 테이블에 group_count 계산해서 업데이트
            #         # 시작 카운트와 종료 카운트가 내용이 동일하다면 그룹 카운트도 동일한 내용이여야함
            #         sql4 = """
            #                   UPDATE group_all
            #            SET group_count = CASE
            #                                WHEN start_count = end_count THEN end_count
            #                                ELSE end_count
            #                              END
            #            WHERE rider_id = %s;
            #            """
            #         cursor.execute(sql4, (rider_id,))
            #         conn.commit()
            #
            # conn.close()






# group id 나중에 넣어줘야함 종료 시간까지 다 나왔을때


def job():
    print("5분뒤에 다시 실행됩니다.")
    # send()
    reception()

job()

schedule.every(5).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)