import math

# 보험사 아이디 생성
def generate_insu_id(string, random, cursor, oper_id):
    letters_set = string.ascii_letters
    random_list = random.sample(letters_set, 3)
    result_id = ''.join(random_list)
    insu_id = "INSUID_" + result_id
    insu_id_sql = f""" update r_info set insurance_id = '{insu_id}' where oper_id = %s """
    cursor.execute(insu_id_sql, (oper_id))

# r_info insert
def insert_r_info(cursor, oper_id, rider_id, start_time, address, request_company, start_date ):
    sql_not_endtime = f"""SELECT count(*) FROM r_info WHERE rider_id = '{rider_id}' and end_time IS NULL;"""
    cursor.execute(sql_not_endtime)
    result = cursor.fetchone()

    r_info_insert = f"""
       INSERT INTO r_info(oper_id, rider_id, start_time, address, request_company, r_date, u_date)
       SELECT '{oper_id}', '{rider_id}', '{start_time}', '{address}', '{request_company}', NOW(),  NOW() 
       WHERE NOT EXISTS(
           SELECT 1 FROM r_info WHERE rider_id = '{rider_id}'
       AND oper_id = '{oper_id}' ); """
    cursor.execute(r_info_insert)

    if result[0] == 0:
        print("로직-----")
        #  라이더 아이디가 중복되지 않게 저장되야함
        r_info_insert = f""" INSERT INTO group_all (driving_time, start_count, rider_id, end_count, r_date, u_date, group_count)
           SELECT '{start_date}', 0, '{rider_id}', 0, NOW(),  NOW(), 0
           WHERE NOT EXISTS (
               SELECT 1 FROM group_all
               WHERE rider_id = '{rider_id}' AND DATE(driving_time) = DATE('{start_time}')
           );"""
        cursor.execute(r_info_insert)

# start time update
def update_start_time(cursor,rider_id, start_date ):
    sql_min_start_time = f""" SELECT MIN(start_time) FROM r_info WHERE group_id IS NULL AND rider_id = '{rider_id}'; """
    cursor.execute(sql_min_start_time)
    min_start_time = cursor.fetchone()

    if min_start_time[0] is not None:
        group_driving_time = min_start_time[0].strftime("%Y-%m-%d")

        sql_start_count_up = f"""
                           update group_all set start_count = start_count + 1 where rider_id = '{rider_id}' and driving_time like '{group_driving_time}%';
                           """
        cursor.execute(sql_start_count_up)
    else:
        sql_start_count_up = f"""
                           update group_all set start_count = start_count + 1 where rider_id = '{rider_id}' and driving_time like '{start_date}%';
                           """
        cursor.execute(sql_start_count_up)

# end_count update
def update_end_count(rider_id, group_driving_time, cursor):
    sql_update_end_count = f"""
    update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{group_driving_time}%';
            """
    cursor.execute(sql_update_end_count)
    
# 시작시간 체크
def check_time(rider_id, cursor):
    sql_min_start_time = f""" SELECT MIN(start_time) FROM r_info WHERE group_id is NULL AND rider_id = '{rider_id}'; """
    cursor.execute(sql_min_start_time)

# endtime update
def endtime_update(end_datetime, oper_id, cursor):
    sql_update_endtime = f""" UPDATE r_info SET end_time = '{end_datetime}', d_status = 'complete', u_date = NOW() 
      WHERE  oper_id = '{oper_id}';"""
    cursor.execute(sql_update_endtime)

# groupall count
def count_groupall(rider_id, group_min_start_time, cursor):
    sql_count_result = f""" SELECT start_count, end_count from group_all WHERE rider_id = '{rider_id}' and 
    driving_time like '{group_min_start_time}';"""
    cursor.execute(sql_count_result)

# groupall count update
def update_group_count(rider_id,group_min_start_time,cursor ):
    sql_update_group_count = f""" UPDATE group_all SET group_count = group_count + 1 WHERE rider_id = '{rider_id}' 
           and driving_time like '{group_min_start_time}';"""
    cursor.execute(sql_update_group_count)

# 운행 첫 시작시간 확인
def first_time(rider_id, cursor):
    sql_first_start_time_result = f""" select DATE_FORMAT(MIN(start_time), '%Y-%m-%d %T') from r_info
           WHERE group_id is null AND rider_id = '{rider_id}'; """
    cursor.execute(sql_first_start_time_result)

# group id update
def group_id_update(group_id, rider_id, cursor):
    sql_update_group_id = f""" UPDATE r_info SET group_id = '{group_id}' WHERE group_id is null AND rider_id = '{rider_id}'"""
    cursor.execute(sql_update_group_id)

# group_info time check
def date_check(group_id, cursor):
    sql_date_check = f"""
                                            SELECT DATE(start_time), DATE(end_time)
                                            FROM group_info
                                            WHERE group_id = '{group_id}'
                                        """
    cursor.execute(sql_date_check)

# rider_info check
def check_rider_oper(rider_id, cursor):
    sql_rider_oper = f"""
            SELECT min(start_time), max(end_time)
            FROM r_info 
            WHERE rider_id = '{rider_id}'
            GROUP BY DATE(start_time)
            ORDER BY DATE(start_time);;
"""
    cursor.execute(sql_rider_oper)

# d_amount , operating (group_info)
def d_amount(sql, rider_id, cursor, conn, group_id, first_start_time, end_time_s,group_date):
    global oper_m, d_amount_n

    # 운행시간(분) 추출
    c_operating = end_time_s - first_start_time
    minutes = c_operating.total_seconds() / 60
    oper_m = math.ceil(minutes)

    # 기사 해당일자 운행했었던 시간, 금액 전체 sum
    rider_info = f"""
       select IFNULL(sum(c_operating), 0), IFNULL(sum(d_amount), 0) from logic.group_info where rider_id = '{rider_id}'
        and group_id like '%{group_date}%';
    """
    cursor.execute(rider_info)
    result = cursor.fetchone()

    if int(result[0]) > 300:  # 누적 > 300
        print("현재 누적 운행 시간이 300분 초과")
        sql.date_check(group_id, cursor)
        conn.commit()
        d_amount_n = 0

    elif oper_m + int(result[0]) > 300:  # 누적 + 현재 운행 > 300
        # 새로 추가되는 시간이 더해져야함
        print("지금까지의 운행 시간에 현재 운행 시간이 더해져서 300분이 초과")
        d_amount_n = 6700 - result[1]

    else:  # 누적 < 300
        print("운행 시간이 300분이 초과X")
        total_minutes = c_operating.days * 1440 + c_operating.seconds / 60
        d_amount_n = total_minutes * 19

    sql.insert_group_info(group_id, cursor, rider_id, first_start_time, end_time_s, d_amount_n, oper_m)

    return d_amount_n

# group_info insert
def insert_group_info(group_id, cursor, rider_id, first_start_time ,end_time_s,d_amounts, oper_m):

    insert_group_info = f"""
                                 INSERT IGNORE INTO logic.group_info (group_id, rider_id, start_time, end_time ,d_amount, c_operating, u_date, r_date )
                                 SELECT %s,%s,%s,%s,%s,%s,NOW(),NOW()
                                 WHERE NOT EXISTS (
                                 SELECT 1 FROM logic.group_info
                                 WHERE group_id = '{group_id}' ); """
    cursor.execute(insert_group_info, (group_id, rider_id, first_start_time, end_time_s, d_amounts, oper_m))