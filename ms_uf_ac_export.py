# Script for exporting audubon core CSV file representing UF specimens.
#
# Author: Julie Winchester <julia.m.winchester@gmail.com>
# February 14, 2018

import credentials
import ms_media_file
import pandas
import phpserialize
import pymysql
import zlib

def db_conn():
	return pymysql.connect(unix_socket = credentials.db['socket'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor)

def db_query(cursor, sql, args):
	cursor.execute(sql, [args])
	return cursor.fetchall()

specimen_uuids = pandas.read_csv('uf_herp_specimen_uuids.csv')
uuid_list = list(specimen_uuids['uuid'])

conn = db_conn()
c = conn.cursor()

sql = """ SELECT * FROM `ms_specimens` AS s 
		  INNER JOIN `ms_media` AS m ON s.specimen_id = m.specimen_id 
		  INNER JOIN `ms_media_files` AS mf ON m.media_id = mf.media_id
		  INNER JOIN `ms_facilities` AS f ON m.facility_id = f.facility_id
		  INNER JOIN `ms_scanners` AS sc ON m.scanner_id = sc.scanner_id
		  INNER JOIN `ca_users` AS u ON m.user_id = u.user_id
		  WHERE s.uuid IN %s """

r = db_query(c, sql, uuid_list)

ac = pandas.DataFrame(columns=
	['dcterms:identifier', 
	'ac:associatedSpecimenReference',
	'ac:providerManagedID',
	'ac:derivedFrom',
	'ac:providerLiteral',
	'ac:provider',
	'dc:type',
	'dcterms:type',
	'ac:subtypeLiteral',
	'ac:subtype',
	'ac:accessURI',
	'dc:format',
	'ac:subjectPart',
	'ac:subjectOrientation',
	'ac:caption',
	'Iptc4xmpExt:LocationCreated',
	'ac:captureDevice',
	'dc:creator',
	'ms:scanningTechnician',
	'ac:fundingAttribution',
	'exif:Xresolution',
	'exif:Yresolution',
	'dicom:SpacingBetweenSlices',
	'dc:rights',
	'dcterms:rights',
	'xmpRights:Owner',
	'xmpRights:UsageTerms',
	'xmpRights:WebStatement',
	'ac:licenseLogoURL',
	'photoshop:Credit',
	'coreid'])

for mf_dict in r:
	mf = ms_media_file.MsMediaFile(mf_dict)

	if mf.is_published():	
		ac = ac.append(mf.ac_mf_dict, ignore_index=True)
		ac = ac.append(mf.ac_mfp_dict, ignore_index=True)

ac.to_csv('output.csv', index=False, index_label=False)










