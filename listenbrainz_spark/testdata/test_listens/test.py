import json
import unittest
from datetime import datetime
from dateutil.relativedelta import relativedelta

def create_json_files(filename, date):
	with open(filename) as f:
 			file = open('{}.json'.format(date.strftime('%m')), 'w')
 			start_date = date
 			end_date = (start_date + relativedelta(months=1)).replace(day=1)
 			for data in f:
 				listen = json.loads(data)
 				if start_date == end_date:
 					start_date = date
 				listen['listened_at'] = start_date.isoformat() + 'Z'
 				json.dump(listen, file)
 				file.write('\n')
 				start_date += relativedelta(days=1)

end_date = datetime.utcnow().replace(microsecond=0).replace(day=1)
start_date = (end_date + relativedelta(months=-1))

print('end_date', end_date)
print('start_date', start_date)

create_json_files('test_listens_copy1.json', start_date)
create_json_files('test_listens_copy2.json', end_date)
