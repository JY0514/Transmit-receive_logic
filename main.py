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


def send():  # 송신 로직
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
            '', '2024-03-27 16:11:00.000', '2024-03-27 12:16:00.000', '', '2024-03-27 12:44:00.000',
            '2024-03-27 15:50:00.000', '2024-03-27 14:37:00.000', '2024-03-27 16:25:00.000', '2024-03-27 16:21:00.000',
            '2024-03-27 14:56:00.000'
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


# 수신 로직
def reception():
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logic.s_info")
    result = cursor.fetchall()
    data_count = len(result)
    print(f"데이터 갯수: {data_count}")

    # start와 end 구분
    sql = f"""
        select
            rider_id, oper_id,
            start_time as 'time',
            'start' as 'state'
        from
            logic.s_info
        union all
        select
            rider_id, oper_id,
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
        rider_id, oper_id, time, state = row
        if state == 'start':
            print('수신 운행시작')
            #  1. group_all에서 동일한 시간이 있는지 검색한다.
            sql_time_check = f"""
            select rider_id, driving_time from logic.group_all where driving_time = '{time}' 
            """
            cursor.execute(sql_time_check)
            conn.commit()
            resultsss = cursor.fetchall()

            if len(resultsss) == 0:
                r_date =datetime.now()
                # 1) 라이더 아이디가 중복되지 않게 저장되야함
                # driving_time이 제일 이른 시간이 들어가야함 악

                insert_query = f"""
                            INSERT INTO logic.group_all (driving_time,start_count, rider_id, end_count,  r_date, u_date, group_count)
                            SELECT  '{time}', 0,'{rider_id}', 0, '{r_date}',' {r_date}', 0
                            WHERE NOT EXISTS (
                                SELECT 1 FROM logic.group_all
                               WHERE rider_id = '{rider_id}'
                            );
                              """
                cursor.execute(insert_query)
                conn.commit()
                resultq = cursor.fetchall()
                print(resultq)



            else: #2)
                update_query = f"""
                UPDATE logic.group_all SET start_count = start_count + 1  WHERE rider_id ='{rider_id}' ;
                """
                cursor.execute(update_query)
                conn.commit()
                     ㅠ
                # start_count는 group_info에 있는 데이터 갯수만큼 정해지는 것임
                update_query = f"""
                                         UPDATE logic.group_all g
                                            INNER JOIN logic.group_info i ON g.rider_id = i.rider_id
                                            SET g.start_count = g.start_count + 1
                                            WHERE i.rider_id = '{rider_id}';
                                            """
                cursor.execute(update_query)
                conn.commit()

            # 3) s_info에 데이터 조회
            sql_check = f"""select   *  from logic.s_info"""
            cursor.execute(sql_check)
            conn.commit()
            result = cursor.fetchall()
            insu_number = 1

            # r_info 데이터 입력
            for record in result:
                rider_id, oper_id, start_time, end_time, address, request_company = record
                now = datetime.now()
                r_date = now.strftime('%Y-%m-%d %H:%M:%S')
                insu_id = f"TEST{insu_number:05}"
                u_date = datetime.now()

                insert_query = f"""
                           INSERT INTO logic.r_info ( oper_id, rider_id, start_time,  address, request_company,  r_date, u_date)
                            SELECT  '{oper_id}','{rider_id}', '{start_time}', '{address}', '{request_company}',  '{r_date}', '{u_date}'
                            WHERE NOT EXISTS (
                                SELECT 1 FROM logic.r_info
                                WHERE rider_id = '{rider_id}' AND oper_id = '{oper_id}'
                            );
                        """
                cursor.execute(insert_query)
                conn.commit()
                insu_number += 1

        else:  # state == 'end'
            print('수신(운행종료, 그룹핑)')
            # 1. group_couont +1
            sql_endcount = f"""
             UPDATE logic.group_all SET end_count = 1 WHERE rider_id ='{rider_id}' ;
            """
            cursor.execute(sql_endcount)
            conn.commit()

            # 3.4. start_count랑 end_count가 동일한지
            sql_allupdate = f"""
                                   SELECT CASE
                                   WHEN start_count = end_count THEN 1
                                   ELSE 0
                                   END AS 일치여부
                            FROM group_all
                            WHERE rider_id = %s;
                                """

            cursor.execute(sql_allupdate, (rider_id))
            conn.commit()
            resultssss = cursor.fetchall()
            print(resultssss)

            if resultssss == '1':
              print("이미 업데이트 됨")
            else :
                sql_update = f"""
                                UPDATE logic.group_all SET group_count = 1 WHERE rider_id ='{rider_id}'
                            """
                cursor.execute(sql_update)
                conn.commit()

            #  s_info에 데이터 조회 및 end_time 업데이트
            sql_check = f"""select  oper_id, end_time  from logic.s_info"""
            cursor.execute(sql_check)
            conn.commit()
            result = cursor.fetchall()

            for row in result:
                oper_ids, end_times = row
                # end_times 값이 존재할 때만 UPDATE 구문 실행
                if end_times is not None:
                    sql_r_info_up = f"""
                        UPDATE logic.r_info SET end_time = '{end_times}', u_date = NOW() WHERE oper_id ='{oper_ids}';
                    """
                    cursor.execute(sql_r_info_up)
                    conn.commit()


            #5.다건 배송이 종료될때 그룹 아이디 생성
            # group id는 종료 시간까지 다 나왔을때 입력해줘야한다. (그래서 state == end일때 구한다.)
            # group_id 조회(같은 라이더 중에서 배달 시간이 겹치는 부분이 있으면 group_id로 묶는다)
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

            insu_number = 1
            # 6, 앞에서 구한 group_id를 r_info에 업데이트
            for row in results:
                rider_id, end_time, group_id, start_time = row
                sql_update_groupid = f"""
                UPDATE logic.r_info SET group_id = %s WHERE rider_id = %s and end_time = %s and start_time = %s
                """
                cursor.execute(sql_update_groupid, (group_id, rider_id, end_time, start_time))
                conn.commit()

            # 7. d_status == complete 일 경우에만 group_info해준다 (배달 완료된 건에 대해서만) / 운행시간 및 보험료 계산
            sql = f"""
            select group_id, rider_id, start_time, end_time, oper_id from logic.r_info
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
                    insu_id = f"TEST{insu_number:05}"
                    up_query = f"""
                                     UPDATE logic.r_info
                                        SET d_status = CASE  
                                         WHEN '{end_time}' IS NULL OR '{end_time}' = '' THEN 'driving' 
                                                        ELSE 'complete' 
                                                        END,
                                            insurance_id = '{insu_id}'
                                        WHERE rider_id = '{rider_id}' """  # 특정 레코드를 대상으로 하는 조건 추가
                    cursor.execute(up_query)
                    conn.commit()
                    insu_number += 1

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


                    # # start_count 데이터 입력
                    # sql = f"SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;"
                    # cursor.execute(sql)
                    # conn.commit()
                    # result_d = cursor.fetchall()
                    #
                    # for tuple in result_d:
                    #     start_count = tuple[0]
                    #     rider_ids = tuple[1]
                    #     # start_time이 몇개 인지 확인한 위에서 받은 결과를 group_all 테이블 업데이트
                    #     sql2_update = f"""
                    #                                 update group_all
                    #                                  set start_count  = '{start_count}'
                    #                                  where group_all.rider_id = '{rider_ids}'
                    #                              """
                    #     cursor.execute(sql2_update)
                    #     conn.commit()

                    # # end_count랑  group_count업데이트하려고 불러옴
                    # ql3_update = """
                    #               SELECT COUNT(*), rider_id
                    #                  FROM logic.r_info
                    #                  WHERE end_time GROUP BY rider_id;
                    #                      """
                    # cursor.execute(ql3_update)
                    # conn.commit()
                    # resultu = cursor.fetchall()
                    # for row in resultu:
                    #     end_count, rider_idss = row
                    #
                    #     # group_all end_count업데이트
                    #     ql4_end_count = """
                    #            UPDATE group_all SET end_count = %s WHERE rider_id = %s
                    #            """
                    #     cursor.execute(ql4_end_count, (end_count, rider_idss))
                    #     conn.commit()
                    #



def job():
    print("5분뒤에 다시 실행됩니다.")
    # send()
    reception()


# 작업 실행
job()

schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
