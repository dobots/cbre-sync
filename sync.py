import sys;
import json;
import requests;
import time;
import md5;
import senseapi;
import os;
import getpass;

from pprint import pprint;

# crownstone_base_url = "http://0.0.0.0:3000"
crownstone_base_url = "http://crownstone-cloud.herokuapp.com"
crownstone_api_url = "%s/api" %crownstone_base_url

sense_api_url = "https://api.sense-os.nl"

date_time_format = "%Y-%m-%dT%H:%M:%S.000Z"

sleep_time = 30

def loginCrownstone():
	response = requests.post("%s/users/login" %crownstone_api_url,
		data = {"email": crownstone_user, "password": crownstone_password})

	if response.status_code != 200:
		print "failed to login"
		raise Exception("failed to login to crownstone cloud")

	global access_token
	access_token = json.loads(response.text)['id']

def getBeacons():
	global access_token

	beacon_filter = '{"fields":["address", "id"]}'

	response = requests.get("%s/Beacons?filter=%s&access_token=%s" %(crownstone_api_url, beacon_filter, access_token))

	if response.status_code != 200:
		print "failed to get beacons"
		return None

	return response.json()

# def getScans():
# 	global access_token

# 	# scan_filter = '{"include":"scans"}'
# 	scan_filter = '{"include":{"relation":"scans", "scope": {"where": {"timestamp": {"gt": "2015-12-18T12:32:30.000Z"}}}}}'

# 	response = requests.get("%s/Beacons?filter=%s&access_token=%s" %(crownstone_api_url, scan_filter, access_token))

# 	if response.status_code != 200:
# 		print "failed to get beacons"
# 		return None

# 	parsed_beacons = [b for b in response.json() if b['scans']]

# 	return parsed_beacons

def getBeaconWithScans(id, timestamp):
	global access_token

	if timestamp:
		scan_filter = '{"include":{"relation":"scans", "scope": {"where": {"timestamp": {"gt": "%s"}}}}}' %timestamp
	else:
		scan_filter = '{"include": "scans"}'

	# print scan_filter

	response = requests.get("%s/Beacons/%s?filter=%s&access_token=%s" %(crownstone_api_url, id, scan_filter, access_token))

	if response.status_code != 200:
		print "failed to get beacons"
		return None

	return response.json()

def loginSense():

	if not api.Login(sense_user, sense_password):
		print "failed to log into sense"
		raise Exception("failed to log into sense")

def getSensor(address):

	if not api.getAllSensors():
		print "failed to get sensors"
		return None

	sensors = getSenseResponse();

	for s in sensors['sensors']:
		if s['name'] == address:
			return s

	return None

def getSensorId(address):
	if sense_sensor_dict[address].has_key("sensorId"):
		return sense_sensor_dict[address]['senseSensorId']
	else:
		sensor = getSensor(address)
		if sensor:
			sense_sensor_dict[address]['senseSensorId'] = sensor['id']
			return sensor['id']
		else:
			print "failed to get sensor id for [%s]" %address
			return None

def createSensor(address):

	if not api.SensorsPost({'sensor': {'name':address, 'device_type':'DoBeacon', 'data_type':'json'}}):
		print "failed to add sensor"
		return

	sensor = getSenseResponse();

	# print "new sensor with id %s" %sensor['sensor']['id']
	sense_sensor_dict[address]['senseSensorId'] = sensor['sensor']['id']

def getSenseResponse():
	return json.loads(api.getResponse())

def uploadSensorData(sensorId, scanData):

	data = []
	for s in scanData:
		value = {'timestamp': s['timestamp'], 'scannedDevices': s['scannedDevices']}
		date = time.mktime(time.strptime(s['timestamp'], date_time_format))
		data.append({'value': value, 'date': date })

	sensorPost = {
		'sensors': [
			{
				'sensor_id': sensorId,
				'data': data
			}
		]
	}

	if not api.SensorsDataPost(sensorPost):
		print "failed to upload sensor data"
		return False

	return True

def getLastUploadTime(address):

	sensorId = getSensorId(address)

	# print "sensorId: %s" %sensorId

	if not api.SensorDataGet(sensorId, {'last':True}):
		print "[%s] failed to get sensor data" %address
		return None

	sensorData = getSenseResponse()

	# print "sensorData: %s" %sensorData

	if sensorData['data']:
		value = json.loads(sensorData['data'][0]['value'])
		return value['timestamp']

	return None

def checkCredentials():
	global crownstone_user, crownstone_password, sense_user, sense_password

	try:
		crownstone_user = os.environ['CROWNSTONE_USER']
	except KeyError:
		print "missing crownstone user name"
		print "(might want to set it as environement variable CROWNSTONE_USER)"
		print "User: ",
		crownstone_user = sys.stdin.readline()[0:-1]
		print

	try:
		crownstone_password = os.environ['CROWNSTONE_PASSWORD']
	except KeyError:
		print "missing crownstone password"
		print "(might want to set it as environement variable CROWNSTONE_PASSWORD?)"
		# crownstone_password = sys.stdin.readline()[0:-1]
		crownstone_password = getpass.getpass()
		print

	try:
		sense_user = os.environ['SENSE_USER']
	except KeyError:
		print "missing sense user name"
		print "(might want to set it as environement variable SENSE_USER?)"
		sense_user = sys.stdin.readline()[0:-1]
		print

	try:
		sense_password = os.environ['SENSE_PASSWORD']
	except KeyError:
		print "missing sense password"
		print "(might want to set it as environement variable SENSE_PASSWORD?)"
		# sense_password = sys.stdin.readline()[0:-1]
		sense_password = getpass.getpass()
		print

def checkForNewBeacons():
	global sense_sensor_dict

	beacons = getBeacons()
	for b in beacons:
		address = b['address']

		if not sense_sensor_dict.has_key(address):
			print "found new beacon [%s]" %address
			sense_sensor_dict[address] = {'crownstoneSensorId': b['id']}

			# using sys.stdout.write and .flush so that it's being written before waiting for getSensorId to complete
			# if using print instead, line is only written after getSensorId completes
			sys.stdout.write("checking last upload time ... ")
			sys.stdout.flush()

			if not getSensorId(address):
				print "creating sensor ... ",
				createSensor(address)
				print "done, id: %s" %sense_sensor_dict[address]['senseSensorId']
				sense_sensor_dict[address]['lastUploadTime'] = None
			else:
				sense_sensor_dict[address]['lastUploadTime'] = getLastUploadTime(address)
				print sense_sensor_dict[address]['lastUploadTime']

			print

sense_sensor_dict = {}

if __name__ == "__main__":

	try:
		print "******************************************************"
		print "* Starting Sync of Crownstone cloud data to Sense DB *"
		print "******************************************************"
		print

		checkCredentials()

		### FIRST TIME

		print "logging in to crownstone cloud ...",
		loginCrownstone()
		print "done"

		global api
		print "logging in to sense ...",
		api = senseapi.SenseAPI()
		loginSense()
		print "done"

		print

		beacons = getBeacons()
		for b in beacons:
			address = b['address']
			print "initializing beacon [%s]" %address
			sense_sensor_dict[address] = {'crownstoneSensorId': b['id']}

			# using sys.stdout.write and .flush so that it's being written before waiting for getSensorId to complete
			# if using print instead, line is only written after getSensorId completes
			sys.stdout.write("checking last upload time ... ")
			sys.stdout.flush()

			if not getSensorId(address):
				print "creating sensor ... ",
				createSensor(address)
				print "done, id: %s" %sense_sensor_dict[address]['senseSensorId']
				sense_sensor_dict[address]['lastUploadTime'] = None
			else:
				sense_sensor_dict[address]['lastUploadTime'] = getLastUploadTime(address)
				print sense_sensor_dict[address]['lastUploadTime']

			print

		# pprint(sense_sensor_dict)
		# print

		print "initialization done, start syncing ..."
		print

		### ITERATE

		iteration = 1

		while True:
			print "%s, starting iteration %s" %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), iteration)
			has_updates = False
			update_count = 0

			checkForNewBeacons()

			for key, value in sense_sensor_dict.items():

				# write output on same line, but clear old output on line first
				sys.stdout.write('\r                                                '.format(iteration))
				sys.stdout.write('\rchecking [{0}] ... '.format(key))
				sys.stdout.flush()

				beacon = getBeaconWithScans(value['crownstoneSensorId'], value['lastUploadTime'])

				if beacon['scans']:
					has_updates = True
					update_count += 1
					print "found %s new scan(s): " %len(beacon['scans'])

					# pprint(beacon['scans'])

					# using sys.stdout.write and .flush so that it's being written before waiting for uploadSensorData
					# to complete
					sys.stdout.write("starting upload ... ")
					sys.stdout.flush()

					if uploadSensorData(value['senseSensorId'], beacon['scans']):
						print "done"
						sense_sensor_dict[key]['lastUploadTime'] = beacon['scans'][-1]['timestamp']
					else:
						print "failed"
				else:
					sys.stdout.write('nothing'.format(key))
					sys.stdout.flush()

			if not has_updates:
				# write output on same line
				sys.stdout.write('\r                                                '.format(iteration))
				sys.stdout.write('\rno updates found\n')
				sys.stdout.flush()
			else:
				# clear output on line
				sys.stdout.write('\r                                                '.format(iteration))
				sys.stdout.flush()

			sys.stdout.write('\r{0}, iteration finished, {1} beacons updated\n\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), update_count))
			sys.stdout.flush()
			# print

			iteration += 1

			time.sleep(sleep_time)

	except KeyboardInterrupt:
		print "\n\nexiting ..."

