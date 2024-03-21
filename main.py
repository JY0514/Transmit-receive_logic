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

        insert_query = f"""
        INSERT INTO logic.r_info (group_id, rider_id, oper_id, start_time, end_time, address, request_company, d_status, insurance_id, r_date, u_date)
        SELECT CONCAT('group', '{rider_id}') AS group_id, '{rider_id}', '{oper_id}', '{start_time}', {end_time_value}, '{address}', '{request_company}', 
        CASE WHEN {end_time_value} IS NOT NULL THEN 'complete' ELSE 'driving' END, '{insu_id}', '{r_date}', '{r_date}';
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
        # print(row)
        rider_id = row[0]
        group_id = row[1]
        start_time = row[2]
        end_time = row[3]
        sql3 = ("insert into logic.group_info ( group_id, rider_id, start_time, end_time, r_date, u_date) "
                f"values('{group_id}','{rider_id}' , '{start_time}','{end_time}','{r_date}','{r_date}')")
        cursor.execute(sql3)

    conn.commit()

    print(group_id)
    sql_group = f"SELECT * FROM logic.group_info WHERE group_id = '{group_id}'"
    cursor.execute(sql_group)
    g_data = cursor.fetchall()
    print(g_data)

    if not g_data:
        print('데이터가 존재하지 않음')

    else:
        print("데이터 존재함")
        sql_update = (f"UPDATE logic.group_info gi SET gi.start_time = (SELECT MAX(ri.start_time)"
                      f" FROM r_info ri WHERE ri.rider_id = gi.rider_id),"
                      f" gi.end_time = (SELECT MAX(ri.end_time) FROM r_info ri WHERE ri.rider_id = gi.rider_id), gi.u_date = NOW()"
                      f" WHERE gi.group_id = '{group_id}'")
    cursor.execute(sql_update)
    conn.commit()

# 없는 경우에 r_info 테이블에서 그 라이더의 정보를 가져와서 운행데이터를 추가해야한다.

# 그리고 보험사 전용 call_id 를 생성하면서 정보를 업데이트하고

# 업데이트 완료된 데이터를 call 테이블에insert한다.


send()
recep()

# 수신 종료 로직

# 라이더에 대해 end_ount _+1을 한다.

# 라이더의 해당하는 call 의 종료시간과 운행 상태를 업데이트를 계속 한다.


# 만약 동일하다면 end_count의 값이 1이 +되고  그와 동시에 운행상태가 업데이트된다.

# 그리고 # 업데이트를 하고나서 end_count, start_count가 group_all테이블에 있는것과 동일한지 체크한다.

# 해당 기사에 대해 gruoup_count + 1을 한다.

# groupid를 채번을 하고

# 채번한 그룹아이디가 다건이기때문에 call roawData에 ㅇ업데이트하여 추가한다.

#  그리고 그룹 아이디ㅡㄹ 전부 조회한다.

#  시작 시간 산출 후 가장 늦은 시간을 변수에 담고

# 시작시간/종료시간/운행시간 으로 보험료를 산출한다.

# groupall_info에 최종 데이터를 저장한다.

#     그룹 수신에서 사용할 코드
