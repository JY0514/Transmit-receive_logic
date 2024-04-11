
# 보험사 아이디 생성
def generate_insu_id(string, random, cursor, oper_id):
    letters_set = string.ascii_letters
    random_list = random.sample(letters_set, 3)
    result_id = ''.join(random_list)
    insu_id = "INSUID_" + result_id
    up_query2 = f""" update r_info set insurance_id = '{insu_id}' where oper_id = %s """
    cursor.execute(up_query2, (oper_id))

# r_info 테이블에 입력
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

# 시작시간 업데이트
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

# end_count 업데이트
def update_end_count(rider_id, group_driving_time, cursor):
    sql_update_endcount = f"""
    update group_all set end_count = end_count + 1 where rider_id = '{rider_id}' and driving_time like '{group_driving_time}%';
            """
    cursor.execute(sql_update_endcount)
    
# 시작시간 체크   
def check_time(rider_id, cursor):
    sql_min_start_time = f""" SELECT MIN(start_time) FROM r_info WHERE group_id is NULL AND rider_id = '{rider_id}'; """
    cursor.execute(sql_min_start_time)

# 운행종료시간 업데이트
def endtime_update(end_datetime, oper_id, cursor):
    sql_update_endtime = f""" UPDATE r_info SET end_time = '{end_datetime}', d_status = 'complete', u_date = NOW() 
      WHERE  oper_id = '{oper_id}';"""
    cursor.execute(sql_update_endtime)

# groupall 카운트
def count_groupall(rider_id, group_min_start_time, cursor):
    sql_count_result = f""" SELECT start_count, end_count from group_all WHERE rider_id = '{rider_id}' and 
    driving_time like '{group_min_start_time}';"""
    cursor.execute(sql_count_result)

# groupall 카운트 업데이트
def update_group_count(rider_id,group_min_start_time,cursor ):
    sql_update_group_count = f""" UPDATE group_all SET group_count = group_count + 1 WHERE rider_id = '{rider_id}' 
           and driving_time like '{group_min_start_time}';"""
    cursor.execute(sql_update_group_count)

# 운행 첫 시작시간 확인
def first_time(rider_id, cursor):
    sql_first_start_time_result = f""" select DATE_FORMAT(MIN(start_time), '%Y-%m-%d %T') from r_info
           WHERE group_id is null AND rider_id = '{rider_id}'; """
    cursor.execute(sql_first_start_time_result)

# 그룹아이디 업데이트
def groupid_update(group_id, rider_id, cursor):
    sql_update_group_id = f""" UPDATE r_info SET group_id = '{group_id}' WHERE group_id is null AND rider_id = '{rider_id}'"""
    cursor.execute(sql_update_group_id)


# 라이더 정보 체크
def check_rider_oper(rider_id, cursor):
    sql_rider_oper = f"""
               SELECT DATE(start_time), SUM(c_operating), SUM(d_amount)
               FROM group_info
               WHERE rider_id = '{rider_id}'
               GROUP BY DATE(start_time)
               ORDER BY DATE(start_time);"""
    cursor.execute(sql_rider_oper)

# group_info 시간 체크
def date_check(group_id, cursor):
    sql_date_check = f"""
                                            SELECT DATE(start_time), DATE(end_time)
                                            FROM group_info
                                            WHERE group_id = '{group_id}'
                                        """
    cursor.execute(sql_date_check)

def info_update(first_start_time, end_time_s, d_amounts, oper_m, group_id, cursor):
    sql_info_update = f"""
                                                 UPDATE group_info
                                                 SET start_time = '{first_start_time}', end_time = '{end_time_s}',
                                                 d_amount = '{d_amounts}', c_operating = '{oper_m}' , u_date = NOW() , r_date = NOW()
                                                  WHERE group_id = '{group_id}' 
                                             """
    cursor.execute(sql_info_update)
def d_amounts(sql, rider_id,cursor, d_amount, conn, oper_m, group_id, first_start_time, end_time_s):
    # 라이더마다 운행시간, 운행시작시간, 총 보험료 확인
    sql.check_rider_oper(rider_id, cursor)
    conn.commit()
    result = cursor.fetchall()

    for row in result:
        date, oper, d = row
        oper_time = oper - oper_m  # 현재 추가되는 운행 제외하고 기존에 있던 시간
        d_amount_now = d - d_amount  # 현재 추가되는 운행 제외하고 기존에 있던 보험료
        time = oper - oper_time  # 현재 업데이트하려는 시간
        for date in row:
            if oper_time > 300:  # 현재 누적 운행 시간이 300이 넘을 때
                print("현재 누적 운행 시간이 300분 초과")
                sql.date_check(group_id, cursor)
                conn.commit()
                date_result = cursor.fetchall()

                # start_time과 end_time 날짜가 다른 경우
                if len(date_result) != 1 or date_result[0][0] != date_result[0][1]:
                    d_amount_n = 0
                    sql.info_update(first_start_time, end_time_s, d_amount_n, oper_m, group_id, cursor)
                    conn.commit()

                else:  # start_time과 end_time 날짜가 같은 경우
                    d_amount_n = 0
                    sql.info_update(first_start_time, end_time_s, d_amount_n, oper_m, group_id, cursor)
                    conn.commit()

            elif time + oper_time > 300:  # 지금 추가되는 시간이 300이 넘는지
                # 새로 추가되는 시간이 더해져야함
                print("지금까지의 운행 시간에 현재 운행 시간이 더해져서 300분이 초과")
                d_amount_n = 6700 - d_amount_now
                sql.info_update(first_start_time, end_time_s, d_amount_n, oper_m, group_id, cursor)
                conn.commit()

            else:  # 운행시간이 300이 안넘음
                print("운행 시간이 300분이 초과X")
                sql.info_update(first_start_time, end_time_s, d_amount, oper_m, group_id, cursor)
                conn.commit()
            return(d_amount, d_amount_n)

def insert_group_info(group_id, cursor, rider_id,first_start_time ,end_time_s,d_amounts,oper_m ):
    sql_info = f"""
                                 INSERT IGNORE INTO logic.group_info (group_id, rider_id,start_time,end_time,d_amount,c_operating, u_date, r_date )
                                 SELECT %s,%s,%s,%s,%s,%s,NOW(),NOW()
                                 WHERE NOT EXISTS (
                                 SELECT 1 FROM logic.group_info
                                 WHERE group_id = '{group_id}' ); """
    cursor.execute(sql_info, (group_id, rider_id,first_start_time,end_time_s, d_amounts, oper_m))
