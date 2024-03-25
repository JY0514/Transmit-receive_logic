import pymysql
from datetime import datetime
from datetime import timedelta
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


# 송신 로직
def send():
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute("SELECT rider_id, oper_id, start_time, end_time, address, request_company FROM logic.s_info")
    result = cursor.fetchall()
    data_count = len(result)
    print(f"데이터 갯수: {data_count}")

    insu_number = 1

    for record in result:
        rider_id, oper_id, start_time, end_time, address, request_company = record
        now = datetime.now()
        r_date = now.strftime('%Y-%m-%d %H:%M:%S')

        end_time_value = 'NULL' if end_time is None else f"'{end_time}'"
        insu_id = f"TEST{insu_number:05}"
        end_time_value = end_time if end_time is not None else '0000-00-00 00:00:00'

        insert_query = f"""
                    INSERT INTO logic.r_info (group_id, rider_id, oper_id, start_time, end_time, address, request_company, d_status, insurance_id, r_date, u_date)
                    SELECT CONCAT('group', '{rider_id}') AS group_id, '{rider_id}', '{oper_id}', '{start_time}', '{end_time_value}', '{address}', '{request_company}',
                    CASE WHEN '{end_time_value}' != '0000-00-00 00:00:00' THEN 'complete' ELSE 'driving' END, '{insu_id}', '{r_date}', '{r_date}'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM logic.r_info
                        WHERE rider_id = '{rider_id}' AND oper_id = '{oper_id}'
                    );
                """

        cursor.execute(insert_query)
        conn.commit()
        insu_number += 1
    conn.close()

#     수신시작 로직
def recep():
    conn = dbconnect()
    cursor = conn.cursor()
    now = datetime.now()
    r_date = now.strftime('%Y-%m-%d %H:%M:%S')

    # rider_id와 group_id별로 최소 start_time과 최대 end_time 가져온다
    sql2 = """
    SELECT rider_id, group_id, MIN(start_time) AS earliest_start_time, MAX(end_time) AS latest_end_time
    FROM logic.r_info
    GROUP BY rider_id, group_id;
    """
    cursor.execute(sql2)
    result2 = cursor.fetchall()

    for row in result2:
        rider_id, group_id, start_time, end_time = row

        cursor.execute("SELECT TIMESTAMPDIFF(MINUTE, %s, %s)", (start_time, end_time))
        c_operating_minutess = cursor.fetchone()[0]
        c_operating_minutes = c_operating_minutess if c_operating_minutess is not None else 0

        # 분당 19원으로 계산
        d_amount = c_operating_minutes * 19 + "원"

        # group_info 테이블에 데이터 삽입
        sql3 = """
        INSERT INTO logic.group_info (c_operating, group_id, rider_id, start_time, end_time, r_date, u_date)
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM logic.group_info
            WHERE group_id = %s AND rider_id = %s
        );
        """
        cursor.execute(sql3,
                       (c_operating_minutes, group_id, rider_id, start_time, end_time, r_date, r_date, group_id,
                        rider_id))
        conn.commit()


        # group_info에서 총 운행 시간에 대한 가격 업데이트
        update_query = "UPDATE logic.group_info SET d_amount = %s WHERE rider_id = %s AND group_id = %s"
        cursor.execute(update_query, (d_amount, rider_id, group_id))
        conn.commit()

        # group_all 테이블에 데이터 삽입
        sql33 = """
        INSERT INTO logic.group_all (driving_time, rider_id, r_date, u_date, group_count)
        SELECT %s, %s, %s, %s, 0
        WHERE NOT EXISTS (
            SELECT 1 FROM logic.group_all
            WHERE rider_id = %s
        );
        """
        cursor.execute(sql33, (c_operating_minutes, rider_id, r_date, r_date, rider_id))
        conn.commit()

        # 기본적으로 end_count 0으로 입력
        ql2_update = """
        UPDATE group_all 
        SET end_count = 0
        WHERE rider_id = %s
        """
        cursor.execute(ql2_update, (rider_id,))
        conn.commit()

        # end_count 내용 업데이트
        ql3_update = """
        SELECT COUNT(*), rider_id
        FROM r_info
        WHERE start_time IS NOT NULL AND end_time IS NOT NULL
        GROUP BY rider_id;
        """
        cursor.execute(ql3_update)
        resultu = cursor.fetchall()

        for row in resultu:
            end_count, rider_idss = row
            ql4_update = """
            UPDATE group_all SET end_count = %s WHERE rider_id = %s
            """
            cursor.execute(ql4_update, (end_count, rider_idss))
            conn.commit()

            # group_all 테이블에 group_count 계산해서 업데이트
            # 시작 카운트와 종료 카운트가 내용이 동일하다면 그룹 카운트도 동일한 내용이여야함
            sql4 = """
            UPDATE group_all
            SET group_count = end_count
            WHERE start_count = end_count AND rider_id = %s
            """
            cursor.execute(sql4, (rider_id,))
            conn.commit()

    conn.close()



def job():
    print("5분뒤에 다시 실행됩니다.")
    send()
    recep()

job()

schedule.every(5).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
