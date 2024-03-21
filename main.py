import pymysql
from datetime import datetime


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
    # 데이터베이스 연결
    conn = dbconnect()
    cursor = conn.cursor()
    # s_info에 담겨있는 전체 데이터 조회
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

        # insert_query = f"""
        # INSERT INTO logic.r_info (group_id, rider_id, oper_id, start_time, end_time, address, request_company, d_status, insurance_id, r_date, u_date)
        # SELECT CONCAT('group', '{rider_id}') AS group_id, '{rider_id}', '{oper_id}', '{start_time}', {end_time_value}, '{address}', '{request_company}',
        # CASE WHEN {end_time_value} IS NOT NULL THEN 'complete' ELSE 'driving' END, '{insu_id}', '{r_date}', '{r_date}';
        # """
        insert_query = f"""
            INSERT INTO logic.r_info (group_id, rider_id, oper_id, start_time, end_time, address, request_company, d_status, insurance_id, r_date, u_date)
            SELECT CONCAT('group', '{rider_id}') AS group_id, '{rider_id}', '{oper_id}', '{start_time}', {end_time_value}, '{address}', '{request_company}',
            CASE WHEN {end_time_value} IS NOT NULL THEN 'complete' ELSE 'driving' END, '{insu_id}', '{r_date}', '{r_date}'
            WHERE NOT EXISTS (
                SELECT 1 FROM logic.r_info
                WHERE rider_id = '{rider_id}' AND oper_id = '{oper_id}'
            );
        """

        cursor.execute(insert_query)
        conn.commit()
        insu_number += 1

    # 데이터베이스 연결 종료
    conn.close()


#     수신시작 로직
def recep():
    conn = dbconnect()
    cursor = conn.cursor()
    cursor.execute("SELECT group_id,rider_id, start_time, end_time, r_date, u_date FROM logic.r_info")
    sql2 = " SELECT rider_id, group_id,MIN(start_time) AS earliest_start_time, MAX(end_time) AS latest_end_time FROM  logic.r_info GROUP BY rider_id;"
    cursor.execute(sql2)
    result2 = cursor.fetchall()
    now = datetime.now()
    r_date = now.strftime('%Y-%m-%d %H:%M:%S')
    for row in result2:
        rider_id = row[0]
        group_id = row[1]
        start_time = row[2]
        end_time = row[3]

        c_operating = end_time - start_time
        sql3 = f"""
        INSERT INTO logic.group_info (c_operating, group_id, rider_id, start_time, end_time, r_date, u_date)
        SELECT '{c_operating}', '{group_id}', '{rider_id}', '{start_time}', '{end_time}', '{r_date}', '{r_date}'
        WHERE NOT EXISTS (
        SELECT 1 FROM logic.group_info
        WHERE group_id = '{group_id}' AND rider_id = '{rider_id}'
        );
        """
        cursor.execute(sql3)
        conn.commit()

        sql_check = ("SELECT COUNT(*)"
                     "FROM logic.group_info GROUP BY group_id, rider_id HAVING COUNT(*) > 1;")
        cursor.execute(sql_check)

        sql_update = (f"UPDATE logic.group_info gi SET gi.start_time = (SELECT MAX(ri.start_time)"
                      f" FROM r_info ri WHERE ri.rider_id = gi.rider_id),"
                      f" gi.end_time = (SELECT MAX(ri.end_time) FROM r_info ri WHERE ri.rider_id = gi.rider_id), gi.u_date = NOW()"
                      f" WHERE gi.group_id = '{group_id}'")
        cursor.execute(sql_update)
        conn.commit()
        c_operating = end_time - start_time
        days = c_operating.days
        seconds = c_operating.seconds
        minutes_from_days = days * 24 * 60
        hours_from_seconds = seconds // 3600
        minutes_from_seconds = (seconds % 3600) // 60
        total_minutes = minutes_from_days + hours_from_seconds * 60 + minutes_from_seconds
        d_amount = total_minutes * 20

        update_query = f"UPDATE  logic.group_info  SET d_amount = '{d_amount}' WHERE rider_id = '{rider_id}'"
        cursor.execute(update_query)
        conn.commit()

        sql = f"SELECT rider_id, COUNT(oper_id) AS start_count , r_date FROM logic.r_info GROUP BY rider_id;"
        cursor.execute(sql)
        conn.commit()
        result_d = cursor.fetchall()

        for row in result_d:
            rider_id = row[0]
            start_count = row[1]
            r_dates = row[2]

            cursor.execute("SELECT COUNT(*) FROM logic.group_all WHERE rider_id = %s", (rider_id,))
            result_count = cursor.fetchone()[0]

            sql12 = f"SELECT rider_id, SEC_TO_TIME(SUM(TIMESTAMPDIFF(SECOND, start_time, end_time))) AS total_driving_time FROM logic.r_info ri GROUP BY rider_id;"
            cursor.execute(sql12)
            sql12result = cursor.fetchall()

            sql13 = f""
            cursor.execute(sql13)
            sql13result = cursor.fetchall()
            # end_count는 r_info 테이블에서 starttime endtime이 둘다 존재할때 1씩 늘어나게해야함


            # groupcount는 r_info start_count와 end_count가 둘다 동일한 숫자일때 1씩 생김

            for row in sql12result:
                print("넘어왔음")
                total_driving_time = row[1]
                formatted_time = str(total_driving_time)

                if result_count == 0:
                    sql222 = "INSERT INTO logic.group_all (rider_id, start_count, r_date,u_date, driving_time) VALUES (%s, %s, %s,NOW(), %s)"
                    cursor.execute(sql222, (rider_id, start_count, r_dates, formatted_time))
                    print(formatted_time)   #다 동일함
        conn.commit()

send()
recep()
