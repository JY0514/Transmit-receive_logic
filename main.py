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
            '2024-03-27 11:30', '2024-03-27 11:30', '2024-03-27 14:37', '2024-03-27 14:30', '2024-03-27 14:26',
            '2024-03-27 12:29', '2024-03-27 12:46', '2024-03-27 14:37', '2024-03-27 16:21', '2024-03-27 14:37'
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
            sql_check = f"""select   *  from logic.s_info"""
            cursor.execute(sql_check)
            conn.commit()
            result = cursor.fetchall()
            insu_number = 1

            for record in result:
                rider_id, oper_id, start_time, end_time, address, request_company = record
                now = datetime.now()
                r_date = now.strftime('%Y-%m-%d %H:%M:%S')
                insu_id = f"TEST{insu_number:05}"
                end_time_value = end_time if end_time is not None else '0000-00-00 00:00:00'
                u_date = datetime.now()

                insert_query = f"""
                           INSERT INTO logic.r_info ( oper_id, rider_id, start_time, end_time, address, request_company, d_status, insurance_id, r_date, u_date)
                            SELECT  '{oper_id}','{rider_id}', '{start_time}', '{end_time_value}', '{address}', '{request_company}',
                            CASE WHEN '{end_time_value}' != '0000-00-00 00:00:00' THEN 'complete' ELSE 'driving' END, '{insu_id}', '{r_date}', '{u_date}'
                            WHERE NOT EXISTS (
                                SELECT 1 FROM logic.r_info
                                WHERE rider_id = '{rider_id}' AND oper_id = '{oper_id}'
                            );
                        """
                cursor.execute(insert_query)
                conn.commit()
                insu_number += 1

        else: #  state == 'end':
            print('수신(운행종료, 그룹핑)')
            # group id 나중에 넣어줘야함 종료 시간까지 다 나왔을때 (그래서 state == end일때 구한다.)
            # group_id를 구한다.(같은 라이더 중에서 배달 시간이 겹치는 부분이 있으면 group_id로 묶는다)
            sql_group_id = f"""
                 SELECT rider_id, end_time ,CONCAT('callID_',a.rider_id, a.oper_id) AS group_id, start_time
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

            # 앞에서 구한 group_id를 r_info에 업데이트
            for row in results:
                rider_id, end_time, group_id, start_time = row
                sql_update_groupid = f"""
               UPDATE logic.r_info SET group_id = %s WHERE rider_id = %s and end_time = %s and start_time = %s
                """
                cursor.execute(sql_update_groupid,(group_id, rider_id, end_time, start_time) )
                conn.commit()

            # d_status == complete 일 경우에만 group_info해준다 (배달 완료된 건에 대해서만)
            sql = f"""
            select group_id, rider_id, start_time, end_time,r_date  from logic.r_info where r_info.d_status = 'complete'
            """
            cursor.execute(sql)
            conn.commit()
            result = cursor.fetchall()
            for record in result:
                group_id, rider_id, start_time, end_time, r_date = record
                if record[0] is not None:
                    c_operating = end_time - start_time
                    total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
                    d_amount = total_minutes * 19
                    u_date = datetime.now()

                    # group_info 테이블 데이터 삽입
                    sql_info = f"""
                                      INSERT IGNORE INTO logic.group_info (c_operating, d_amount,group_id, rider_id, start_time, end_time, r_date, u_date)
                                      SELECT %s,%s, %s, %s, %s, %s, %s, %s
                                      WHERE NOT EXISTS (
                                          SELECT 1 FROM logic.group_info
                                          WHERE group_id = %s 
                                      );
                                      """
                    cursor.execute(sql_info,(c_operating, d_amount, group_id, rider_id, start_time, end_time, r_date, u_date, group_id))
                    conn.commit()

                    # group_all 테이블에 데이터 삽입
                    sql_all = f"""
                           INSERT INTO logic.group_all (driving_time, rider_id, r_date, u_date, group_count)
                           SELECT %s, %s, %s, %s, 0
                           WHERE NOT EXISTS (
                               SELECT 1 FROM logic.group_all
                               WHERE rider_id = %s
                           );
                           """
                    cursor.execute(sql_all, (c_operating, rider_id, r_date,u_date,  rider_id))
                    conn.commit()
                         # 기본적으로 end_count 0 입력
                    ql2_update = """
                           UPDATE group_all
                           SET end_count = 0
                           WHERE rider_id = %s
                           """
                    cursor.execute(ql2_update, (rider_id))
                    conn.commit()

                    # start_count 데이터 입력
                    sql = f"SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;"
                    cursor.execute(sql)
                    conn.commit()
                    result_d = cursor.fetchall()

                    for tuple in result_d:
                        start_count = tuple[0]
                        rider_ids = tuple[1]
                        # start_time이 몇개 인지 확인한 결과를 group_all 테이블 업데이트
                        sql2_update = f"""
                                                    update group_all
                                                     set start_count  = '{start_count}'
                                                     where group_all.rider_id = '{rider_ids}'
                                                 """
                        cursor.execute(sql2_update)
                        conn.commit()

                    ql3_update = """
                                  SELECT COUNT(*), rider_id
                                     FROM logic.r_info
                                     WHERE end_time GROUP BY rider_id;
                                         """
                    cursor.execute(ql3_update)
                    conn.commit()
                    resultu = cursor.fetchall()
                    for row in resultu:
                        end_count, rider_idss = row
                        ql4_end_count = """
                               UPDATE group_all SET end_count = %s WHERE rider_id = %s
                               """
                        cursor.execute(ql4_end_count, (end_count, rider_idss))
                        conn.commit()
                            # group_all 테이블에 group_count 계산해서 업데이트
                            # 시작 카운트와 종료 카운트가 내용이 동일하다면 그룹 카운트도 동일한 내용이여야함
                        sql4 = """
                                      UPDATE group_all
                               SET group_count = CASE
                                                   WHEN start_count = end_count THEN end_count
                                                   ELSE end_count
                                                 END
                               WHERE rider_id = %s;
                               """
                        cursor.execute(sql4, (rider_id,))
                        conn.commit()




def job():
    print("5분뒤에 다시 실행됩니다.")
    # send()
    reception()

job()

schedule.every(5).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)