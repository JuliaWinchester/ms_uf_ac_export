# Script for exporting audubon core CSV file representing UF specimens.
#
# Author: Julie Winchester <julia.m.winchester@gmail.com>
# February 14, 2018

import credentials
import pymysql
import pandas

def db_conn():
	return pymysql.connect(host = credentials.db['server'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor)

specimen_uuids = pandas.read_csv('uf_herp_specimen_uuids')
uuid_list = list(specimen_uuids['uuid'])

conn = db_conn()

try:
	with conn.cursor() as cursor:
		sql = "SELECT * FROM `ms_specimens` WHERE `uuid` IN %s"
		cursor.execute(sql, [uuid_list])
		s = cursor.fetchall()
	finally:
		conn.close()


