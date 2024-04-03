# import pymysql
# from flask import Flask, request, json, jsonify
#
# app = Flask(__name__)
#
#
# @app.route("/send", methods=['POST'])
# def versionCheck():
#     params = request.get_json()
#     print("받은 Json 데이터 ", params)
#     response = {
#         "result": "ok"
#     }
#     return jsonify(response)
#
#
# def dbconnect():
#     conn = pymysql.connect(host='127.0.0.1', user='root', password='1234', db='logic', charset='utf8')
#     return conn
#
# try:
#     connection = dbconnect()
#     if connection:
#         print("DB 접속 완료")
#     else:
#         print("DB 접속 실패")
# except Exception as e:
#     print("DB 접속 중 오류가 발생 : ", str(e))
#
#
# def send():  # 송신 로직
#     conn = dbconnect()
#     cursor = conn.cursor()
#
#     # 송신 첫번째
#     sql_send = f"""
#     INSERT INTO logic.s_info (oper_id, rider_id ,  start_time ,   address ,request_company)
#     VALUES
#     ('1','a', '2024-03-27 11:20',  'abc', 'abc'),
#     ('2','b',  '2024-03-27 12:24',  'abc', 'abc'),
#     ('3','a',  '2024-03-27 13:26', 'abc', 'abc'),
#     ('4','c',  '2024-03-27 14:06',  'abc', 'abc'),
#     ('5','b',  '2024-03-27 11:27', 'abc', 'abc'),
#     ('6','c',  '2024-03-27 12:06',  'abc', 'abc'),
#     ('7','b',  '2024-03-27 13:37',  'abc', 'abc'),
#     ('8','b',  '2024-03-27 14:32',  'abc', 'abc'),
#     ('9','b',  '2024-03-27 16:01', 'abc', 'abc'),
#     ('10','a',  '2024-03-27 15:20',  'abc', 'abc')
#     """
#     cursor.execute(sql_send)
#     conn.commit()
#
#     cursor.execute("SELECT  rider_id, end_time FROM logic.s_info")
#     result = cursor.fetchall()
#
#     for i, row in enumerate(result):
#         end_time_values = [
#             '', '2024-03-27 16:11:00.000', '2024-03-27 12:16:00.000', '', '2024-03-27 12:44:00.000',
#             '2024-03-27 15:50:00.000', '2024-03-27 14:37:00.000', '2024-03-27 16:25:00.000', '2024-03-27 16:21:00.000',
#             '2024-03-27 14:56:00.000'
#         ]
#         rider_id, end_time = row
#         # end_time_values 리스트에서 해당 인덱스의 값을 선택
#         end_time_value = end_time_values[i]
#
#         # 송신 end_time 들어가는 부분
#         sql_end = """
#         UPDATE logic.s_info SET end_time = %s WHERE rider_id = %s
#         """
#         cursor.execute(sql_end, (end_time_value, rider_id))
#         conn.commit()
#
