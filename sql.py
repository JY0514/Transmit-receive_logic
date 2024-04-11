import pymysql
def dbconnect():
    conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
    return conn

def generate_insu_id(string, random, cursor, oper_id):
    letters_set = string.ascii_letters
    random_list = random.sample(letters_set, 3)
    result_id = ''.join(random_list)
    insu_id = "INSUID_" + result_id
    up_query2 = f""" update r_info set insurance_id = '{insu_id}' where oper_id = %s """
    cursor.execute(up_query2, (oper_id))

def insert_r_info(cursor, oper_id, rider_id, start_time, address, request_company, start_date ):
    sql_not_endtime = f"""SELECT count(*) FROM r_info WHERE rider_id = '{rider_id}' and end_time IS NULL;"""
    cursor.execute(sql_not_endtime)
    result = cursor.fetchone()

    insert_query = f"""
       INSERT INTO r_info(oper_id, rider_id, start_time, address, request_company, r_date, u_date)
       SELECT '{oper_id}', '{rider_id}', '{start_time}', '{address}', '{request_company}', NOW(),  NOW() 
       WHERE NOT EXISTS(
           SELECT 1 FROM r_info WHERE rider_id = '{rider_id}'
       AND oper_id = '{oper_id}' ); """
    cursor.execute(insert_query)

    if result[0] == 0:
        print("로직-----")
        #  라이더 아이디가 중복되지 않게 저장되야함
        insert_query = f""" INSERT INTO group_all (driving_time, start_count, rider_id, end_count, r_date, u_date, group_count)
           SELECT '{start_date}', 0, '{rider_id}', 0, NOW(),  NOW(), 0
           WHERE NOT EXISTS (
               SELECT 1 FROM group_all
               WHERE rider_id = '{rider_id}' AND DATE(driving_time) = DATE('{start_time}')
           );"""
        cursor.execute(insert_query)

def update_start_time(cursor,rider_id, start_date ):
    sql_min_start_time = f""" SELECT MIN(start_time) FROM r_info WHERE group_id IS NULL AND rider_id = '{rider_id}'; """
    cursor.execute(sql_min_start_time)
    min_start_time = cursor.fetchone()

    if min_start_time[0] is not None:
        group_driving_time = min_start_time[0].strftime("%Y-%m-%d")

        sql_startcount_up = f"""
                           update group_all set start_count = start_count + 1 where rider_id = '{rider_id}' and driving_time like '{group_driving_time}%';
                           """
        cursor.execute(sql_startcount_up)
    else:
        sql_startcount_up = f"""
                           update group_all set start_count = start_count + 1 where rider_id = '{rider_id}' and driving_time like '{start_date}%';
                           """
        cursor.execute(sql_startcount_up)


def r_info_check(oper_id, cursor, datetime, end_time, rider_id):

    sql_r_info = f"""
        select start_time
          from r_info
          where oper_id = '{oper_id}';
    """
    cursor.execute(sql_r_info)

    start_datetime = cursor.fetchone()
    end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    # Date(YYYY-MM-DD 형식 문자열포맷팅)

    start_time_str = start_datetime[0].strftime("%Y-%m-%d")
    global group_min_start_time

    # 그룹(다건)운행 중인지 아닌지에 대한 체크
    sql_min_start_time = f""" SELECT MIN(start_time) FROM r_info WHERE group_id is NULL AND rider_id = '{rider_id}'; """
    cursor.execute(sql_min_start_time)
    min_start_time = cursor.fetchone()


def update_start(min_start_time, rider_id, cursor,start_time_str ):

    if min_start_time[0] is not None:
        group_driving_time = min_start_time[0].strftime("%Y-%m-%d")

        sql_update_endcount = f"""
        update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{group_driving_time}%';
                """
        cursor.execute(sql_update_endcount)

        group_min_start_time = group_driving_time
    else:
        sql_update_endcount = f"""
        update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{start_time_str}%';
                """
        cursor.execute(sql_update_endcount)

        group_min_start_time = start_time_str