from pymongo import MongoClient
from datetime import date
import requests
import hashlib
import creds

client = MongoClient(creds.uri, connectTimeoutMS=30000,socketTimeoutMS=None,socketKeepAlive=True)

db = client.get_default_database()
# db = client.admin
# c = client.list_databases()
# for db in c: 
# 	print(db)

def get_applied_not_accepted():
	users = {}
	for user in db.users.find({"status.completedProfile": True},{"status.admitted":False}):
		users[user['email']]=user['profile']['name']
	return users

# print(get_applied_not_accepted())

def get_accepted_not_confirmed_no_team():
	users = {}
	for user in db.users.find({"$and":[{"status.admitted:": True,"status.confirmed": False,"status.declined": False},{"teamCode":None}]}):
		users[user['email']]=user['profile']['name']
	return users


def get_accepted_not_confirmed_team():
	users = {}
	for user in db.users.find({"$and":[{"status.admitted:": True,"status.confirmed": False,"status.declined": False},{"teamCode": {"$ne": None} }]}):
		users[user['email']]=user['profile']['name']
	return users

def get_accepted_confirmed_no_team():
	users = {}
	for user in db.users.find({"$and":[{"status.confirmed": True,"status.declined": False},{"teamCode":None}]}):
		users[user['email']]=user['profile']['name']
	return users

def get_all_accepted_not_confirmed():
	users = {}
	for user in db.users.find({"$and": [{"status.completedProfile": True,"status.admitted": True,"status.confirmed": False,"status.declined": False}]}):
		users[user['email']]=user['profile']['name']
	return users

# accepted = len(get_all_accepted_not_confirmed())

def get_all_applied_no_team():
	users = {}
	for user in db.users.find({"$and":[{"status.completedProfile": True,"teamCode":None,"status.declined": False}]}):
		users[user['email']]=user['profile']['name']
	return users

def get_all_accepted_no_team():
	users = {}
	for user in db.users.find({"$and":[{"status.admitted": True},{"teamCode":None},{"status.declined": False}]}):
		users[user['email']]=user['profile']['name']
	return users

def get_not_verified():
	users = {}
	for user in db.users.find({"$and":[{"verified": False},{"status.declined": False}]}):
		users[user['email']]=user['email']
	return users

# unverified = len(get_not_verified())

def get_unfinished_application():
	users = {}
	for user in db.users.find({"$and":[{"status.completedProfile": False},{"status.declined": False}]}):
		users[user['email']]=user['email']
	return users

unfinished_app = len(get_unfinished_application());

def get_all_confirmed():
	users = {}
	for user in db.users.find({"$and":[{"status.confirmed": True,"status.declined": False}]}):
		users[user['email']]=user['profile']['name']
	return users

def get_all_accepted():
	users = {}
	for user in db.users.find({"$and":[{"status.admitted:": True,"status.declined": False}]}):
		users[user['email']]=user['profile']['name']
	return users

def create_tag(tag_type):
	tag_name = tag_type + ' - ' + str(date.today().month) + '/' + str(date.today().day)
	print(tag_name)
	new_tag = {
		"name": tag_name,
		"static_segment": []
	}

	r = requests.post(url=creds.url+'/segments/', json=new_tag,auth=creds.auth).json()
	if 'status' in r:
		if r['status'] == 400 and r['detail'] == "Sorry, that tag already exists.": 
			r = requests.get(url=creds.url+'/segments/',auth=creds.auth).json()
			for tag in r['segments']:
				if tag['name'] == tag_name: 
					return tag['id']
		else:
			print('ERR: ' + r['title'])
			print('ERR detail: ' + r['detail'])
			return
	return r['id']

def create_contacts_with_tag(users,tag_id):

	members_to_add = []
	error_emails = []

	for user_email, name in users.items(): 

		user_hash = hashlib.md5(user_email.lower().encode('utf-8')).hexdigest()

		# check if user exists
		r = requests.get(creds.url+'/members/'+str(user_hash)+'/',auth=creds.auth).json()

		# create user if user doesn't exist
		if r['status']==404:
			print('user does not exist: ' + user_email)
			new_contact = {
			    "email_address": user_email,
			    "status": "subscribed",
			    "merge_fields": {
			        "fNAME": name,
			    }
			}

			r = requests.post(creds.url+'/members/',json=new_contact,auth=creds.auth).json()
			if r['status'] != 'subscribed':
				print(r)
				error_emails.append(user_email)
				continue
		else:
			print('user exisits: ' + user_email)

		members_to_add.append(user_email)

	members_to_add_param = {
		'members_to_add': members_to_add,
	}

	r = requests.post(creds.url+'/segments/'+str(tag_id)+'/',json=members_to_add_param,auth=creds.auth).json()

	if 'errors' in r: 
		print(r['errors'])

	return error_emails

def main_func(tag_type):

	# get emails
	users = TAGS_USERS_MAP[tag_type]()
	
	# create tag
	tag_id = create_tag(tag_type)

	# add tag to users
	return create_contacts_with_tag(users,tag_id)

TAGS_USERS_MAP = {
	'Accept': get_applied_not_accepted, 
	'Confirm & Team Reminder': get_accepted_not_confirmed_no_team,
	'Confirm Only Reminder':  get_accepted_not_confirmed_team,
	'Team Only Reminder': get_accepted_confirmed_no_team,
	'All Confirm Reminder': get_all_accepted_not_confirmed,
	'All Team Reminder': get_all_accepted_no_team,
	'Verify Reminder': get_not_verified,
	'Application Reminder': get_unfinished_application,
}

# print('Unverified: ' + str(unverified))
# print('Unfinished: ' + str(unfinished_app))
# print('Applied: '  + str(applied))
# print('Accepted: ' + str(accepted))
# print('Total: ' + str(unverified+unfinished_app+applied+accepted))
# print('Team: ' + str(team))
# main_func('Accept')
# main_func('Confirm & Team Reminder')
# main_func('Confirm Only Reminder')
# main_func('Team Only Reminder')
# main_func('All Confirm Reminder')
# main_func('All Team Reminder')
# main_func('Verify Reminder')
# main_func('Application Reminder')

def test():
	tag_id = create_tag('Test Tag9')

	test_contacts1 = {
		"a@gmail.com":"Johnny Appleseed",
		"b@gmail.com":"Jenny",
		"c@gmail.com":"Karen",
		"dan@gmail.com":"Michael Jordan"
	}

	test_contacts2 = {
		"a@gmail.com":"Johnny Appleseed",
		"e@gmail.com":"Kevin",
		"f@gmail.com":"Jess Doe",
		"dan@gmail.com":"Michael Jordan"
	}

	test_contacts3 = {
		"sarah1@gmail":"Johnny Appleseed",
		"evan1@gmail.com":"Kevin",
		"fred1@gmail.com":"Jess Doe",
		"danny1@gmail.com":"Michael Jordan"
	}
	print(create_contacts_with_tag(test_contacts3, tag_id))


def add_name(): 
	for email,new_name in get_unfinished_application().items():
		user_email=email
		user_hash = hashlib.md5(user_email.lower().encode('utf-8')).hexdigest()

		name = {
		'merge_fields': {
			'FNAME': new_name
		}
		}

		r = requests.patch(creds.url+'/members/'+str(user_hash)+'/',json=name,auth=creds.auth).json()
		print(r)

def print_user(email):
	user_email=email
	r = requests.get(creds.url+'/members/'+str(user_hash)+'/',auth=creds.auth).json()
	print(r)

# add_name()