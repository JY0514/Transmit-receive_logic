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
        test = c_operating.date()
        sql3 = f"""
        INSERT INTO logic.group_info (c_operating, group_id, rider_id, start_time, end_time, r_date, u_date)
        SELECT '{test}', '{group_id}', '{rider_id}', '{start_time}', '{end_time}', '{r_date}', '{r_date}'
        WHERE NOT EXISTS (
        SELECT 1 FROM logic.group_info
        WHERE group_id = '{group_id}' AND rider_id = '{rider_id}'
        );
        """
        cursor.execute(sql3)
        conn.commit()



        # group_all 테이블에 데이터 입력
        sql33 = f"""
        INSERT INTO logic.group_all (driving_time,  rider_id, r_date, u_date, group_count)
        SELECT '{c_operating}', '{rider_id}', '{r_date}', '{r_date}',0
        WHERE NOT EXISTS (
        SELECT 1 FROM logic.group_all
        WHERE rider_id = '{rider_id}' );
        """
        cursor.execute(sql33)
        conn.commit()

        # group_all 테이블에 group_count계산해서 업데이트하기
        # 업데이트도 중복 안되게해야함
        sql4 = f"""
               UPDATE group_all
               SET group_count = group_count + (start_count = end_count)
               WHERE start_count = end_count;
               
               """
        cursor.execute(sql4)
        conn.commit()

        # group_info테이블에서 확인
        sql_check = ("SELECT COUNT(*)"
                     "FROM logic.group_info GROUP BY group_id, rider_id HAVING COUNT(*) > 1;")
        cursor.execute(sql_check)

        # group_info테이블 업데이트
        sql_update = (f"UPDATE logic.group_info gi SET gi.start_time = (SELECT MAX(ri.start_time)"
                      f" FROM r_info ri WHERE ri.rider_id = gi.rider_id),"
                      f" gi.end_time = (SELECT MAX(ri.end_time) FROM r_info ri WHERE ri.rider_id = gi.rider_id), gi.u_date = NOW()"
                      f" WHERE gi.group_id = '{group_id}'")
        cursor.execute(sql_update)
        conn.commit()

        # sql_update3 = f"""
        #                           UPDATE group_all
        #                           SET end_count = CASE
        #                               WHEN start_count > end_count THEN start_count
        #                               ELSE end_count
        #                           END
        #                            WHERE rider_id IN {rider_id};
        #                                        """
        # cursor.execute(sql_update3)
        # conn.commit()

        if c_operating is not None:
            days = c_operating.days
            seconds = c_operating.seconds
            minutes_from_days = days * 24 * 60
            hours_from_seconds = seconds // 3600
            minutes_from_seconds = (seconds % 3600) // 60
            total_minutes = minutes_from_days + hours_from_seconds * 60 + minutes_from_seconds
            d_amount = total_minutes * 19 #분당 19원으로 계산

            update_query = f"UPDATE  logic.group_info  SET d_amount = '{d_amount}' WHERE rider_id = '{rider_id}'"
            cursor.execute(update_query)
            conn.commit()

            # r_info 테이블에서 rider_id 데이터 중에서 end_time이 존재하는거 확인
            sql_check2 = f"""
                                select rider_id from r_info where end_time is null group by rider_id ;
                                """
            cursor.execute(sql_check2)
            conn.commit()

            # r_info 테이블에서 라이더 아이디와 그에 맞는 starttime이 몇개 있는지 확인
            sql = f"SELECT  COUNT(oper_id) AS start_count, rider_id  FROM logic.r_info GROUP BY rider_id;"
            cursor.execute(sql)
            conn.commit()
            result_d = cursor.fetchall()

            for tuple in result_d:
                start_count = tuple[0]
                rider_ids = tuple[1]

                # start_time이 몇개 인지 확인한 결과를 group_all 테이블에 업데이트
                sql2_update = f"""
                           update group_all 
                            set start_count  = '{start_count}'
                            where group_all.rider_id = '{rider_ids}'
                        """
                cursor.execute(sql2_update)
                conn.commit()

                ql2_update = f"""
                                           update group_all 
                                            set end_count  = '0'
                                            where group_all.rider_id = '{rider_ids}'
                                        """
                cursor.execute(ql2_update)
                conn.commit()

                #  end_count 계산할때 r_info에서 시작시간과 운행시간이 둘 다 존재하는지 그게 라이더마다 몇개인지


                # 라이더마다 총 운행시간 뽑는거
                sql12 = f"SELECT rider_id, SEC_TO_TIME(SUM(TIMESTAMPDIFF(SECOND, start_time, end_time))) AS total_driving_time FROM logic.r_info ri GROUP BY rider_id;"
                cursor.execute(sql12)
                conn.commit()

                # sql13 = f"""
                #
                # """
                # cursor.execute(sql13)
                # conn.commit()
                                    # groupcount는 r_info start_count와 end_count가 둘다 동일한 숫자일때 1씩 생김

            #     for row in sql12result:
            #         print("넘어왔음")
            #         total_driving_time = row[1]
            #         formatted_time = str(total_driving_time)
            #
            #         if result_count == 0:
            #             sql222 = "INSERT INTO logic.group_all (rider_id, start_count, r_date,u_date, driving_time) VALUES (%s, %s, %s,NOW(), %s)"
            #             cursor.execute(sql222, (rider_id, start_count, r_dates, formatted_time))
            #             print(formatted_time)  # 다 동일함
            # conn.commit()


send()
recep()
