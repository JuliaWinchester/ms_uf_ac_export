# Script for exporting audubon core CSV file representing UF specimens.
#
# Author: Julie Winchester <julia.m.winchester@gmail.com>
# February 14, 2018

import credentials
import pandas
import phpserialize
import pymysql
import zlib

def db_conn():
	return pymysql.connect(host = credentials.db['server'],
						   user = credentials.db['username'],
						   password = credentials.db['password'],
						   db = credentials.db['db'],
						   charset = 'utf8mb4',
						   cursorclass=pymysql.cursors.DictCursor)

def db_query(cursor, sql, args):
	cursor.execute(sql, [args])
	return cursor.fetchall()

def blob_to_array(blob):
	return phpserialize.unserialize(zlib.decompress(blob))

def creator_string(mf):
	return mf['fname'] + " " + mf['lname'] + " <" + mf['email'] + ">" 

def copyright_permission(mf):
	return {
		0: 'Copyright permission not set',
		1: 'Person loading media owns copyright and grants permission for use of media on MorphoSource',
		2: 'Permission to use media on MorphoSource granted by copyright holder',
		3: 'Permission pending',
		4: 'Copyright expired or work otherwise in public domain',
		5: 'Copyright permission not yet requested'
	}[int(mf['copyright_permission'])]

def copyright_license(mf):
	return {
		0: 'Media reuse policy not set',
		1: 'CC0 - relinquish copyright',
		2: 'Attribution CC BY - reuse with attribution',
		3: 'Attribution-NonCommercial CC BY-NC - reuse but noncommercial',
		4: 'Attribution-ShareAlike CC BY-SA - reuse here and applied to future uses',
		5: 'Attribution- CC BY-NC-SA - reuse here and applied to future uses but noncommercial',
		6: 'Attribution-NoDerivs CC BY-ND - reuse but no changes',
		7: 'Attribution-NonCommercial-NoDerivs CC BY-NC-ND - reuse noncommerical no changes',
		8: 'Media released for onetime use, no reuse without permission',
		20: 'Unknown - Will set before project publication'
	}[int(mf['copyright_license'])]

def copyright_license_uri(mf):
	return {
		0: '',
		1: 'https://creativecommons.org/publicdomain/zero/1.0/',
		2: 'https://creativecommons.org/licenses/by/3.0/',
		3: 'https://creativecommons.org/licenses/by-nc/3.0/',
		4: 'https://creativecommons.org/licenses/by-sa/3.0/',
		5: 'https://creativecommons.org/licenses/by-nc-sa/3.0/',
		6: 'https://creativecommons.org/licenses/by-nd/3.0/',
		7: 'https://creativecommons.org/licenses/by-nc-nd/3.0/',
		8: '',
		20: ''
	}[int(mf['copyright_license'])]

def copyright_license_logo_uri(mf):
	return {
		0: '',
		1: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-nd.eu.png',
		2: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by.png',
		3: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc.png',
		4: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-sa.png',
		5: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-sa.png',
		6: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nd.png',
		7: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-nd.png',
		8: '',
		20: ''
	}[int(mf['copyright_license'])]

def citation_instructions(mf):
	if mf['media_citation_instruction1']:
		return mf['media_citation_instruction1'] + " provided access to these data " + mf['media_citation_instruction2'] + " " + mf['media_citation_instruction3'] + ". The files were downloaded from www.morphosource.org, Duke University."

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
	'coreid']);

for mf in r:
	mf_info = blob_to_array(mf['mf.media'])

	# Create media file dict
	d = {
		'dcterms:identifier': mf['ark'], 
		'ac:associatedSpecimenReference': mf['uuid'],
		'ac:providerManagedID': mf['media_file_id'],
		'ac:derivedFrom': mf['derived_from_media_file_id'],
		'ac:providerLiteral': 'MorphoSource',
		'ac:provider': 'http://www.morphosource.org',
		'dc:type': 'Image',
		'dcterms:type': 'http://purl.org/dc/dcmitype/Image',
		'ac:subtypeLiteral': '', # mf['modality'] when implemented
		'ac:subtype': '', # need to have function to grab this when implemented
		'ac:accessURI': 'http://www.morphosource.org/index.php/Detail/MediaDetail/Show/media_file_id/' + str(mf['media_file_id']),
		'dc:format': mf_info['original']['MIMETYPE'],
		'ac:subjectPart': mf['mf.element'],
		'ac:subjectOrientation': mf['mf.side'],
		'ac:caption': mf['mf.notes'],
		'Iptc4xmpExt:LocationCreated': mf['name'],
		'ac:captureDevice': mf['sc.name'],
		'dc:creator': creator_string(mf),
		'ms:scanningTechnician': mf['scanner_technicians'],
		'ac:fundingAttribution': mf['grant_support'],
		'exif:Xresolution': mf['scanner_x_resolution'],
		'exif:Yresolution': mf['scanner_y_resolution'],
		'dicom:SpacingBetweenSlices': mf['scanner_z_resolution'],
		'dc:rights': copyright_permission(mf),
		'dcterms:rights': copyright_license_uri(mf),
		'xmpRights:Owner': mf['copyright_info'],
		'xmpRights:UsageTerms': copyright_license(mf),
		'xmpRights:WebStatement': copyright_license_uri(mf),
		'ac:licenseLogoURL': copyright_license_logo_uri(mf),
		'photoshop:Credit': citation_instructions(mf),
		'coreid': mf['occurrence_id']
	}

	p_url = "http://www.morphosource.org/media/morphosource/images/" + mf_info['large']['HASH'] + "/" + mf_info['large']['FILENAME']

	# Create media file preview image dict
	p = {
		'dcterms:identifier': p_url, 
		'ac:associatedSpecimenReference': mf['uuid'],
		'ac:providerManagedID': str(mf['media_file_id']) + 'p',
		'ac:derivedFrom': d['dcterms:identifier'],
		'ac:providerLiteral': 'MorphoSource',
		'ac:provider': 'http://www.morphosource.org',
		'dc:type': 'StillImage',
		'dcterms:type': 'http://purl.org/dc/dcmitype/StillImage',
		'ac:subtypeLiteral': 'Graphic',
		'ac:subtype': 'https://terms.tdwg.org/wiki/AC_Subtype_Examples',
		'ac:accessURI': p_url,
		'dc:format': mf_info['large']['MIMETYPE'],
		'ac:subjectPart': mf['mf.element'],
		'ac:subjectOrientation': mf['mf.side'],
		'ac:caption': mf['mf.notes'],
		'Iptc4xmpExt:LocationCreated': mf['name'],
		'ac:captureDevice': mf['sc.name'],
		'dc:creator': creator_string(mf),
		'ms:scanningTechnician': mf['scanner_technicians'],
		'ac:fundingAttribution': mf['grant_support'],
		'exif:Xresolution': mf_info['large']['WIDTH'],
		'exif:Yresolution': mf_info['large']['HEIGHT'],
		'dicom:SpacingBetweenSlices': '',
		'dc:rights': copyright_permission(mf),
		'dcterms:rights': copyright_license_uri(mf),
		'xmpRights:Owner': mf['copyright_info'],
		'xmpRights:UsageTerms': copyright_license(mf),
		'xmpRights:WebStatement': copyright_license_uri(mf),
		'ac:licenseLogoURL': copyright_license_logo_uri(mf),
		'photoshop:Credit': citation_instructions(mf),
		'coreid': mf['occurrence_id']
	}

	ac = ac.append(d, ignore_index=True)
	ac = ac.append(p, ignore_index=True)

ac.to_csv('output.csv', index=False, index_label=False)










