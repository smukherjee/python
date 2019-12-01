"""
Trial for Spire ingestion
"""

import json
import os
import subprocess
from utils.date import date_range
import datetime

dt_start = '2018-11-01'
until = '2019-09-30'
dt_start = '2019-10-01'
until = '2019-10-31'
in_format = '%Y-%m-%d'
out_format = '%Y-%m-%d'
dt_range = date_range(dt_start=dt_start, until=until, in_format=out_format, out_format=out_format)

job_ids = dict()
completed = dict()
with open("spire.txt", "r") as file:
    token = file.read().replace('\n', '')

for dt in dt_range:
    print(dt)
    dt1 = datetime.datetime.strptime(dt, out_format) + datetime.timedelta(days=1)
    dt1 = dt1.strftime(out_format)
    cmd = f"curl -X PUT 'https://api.airsafe.spire.com/archive/job?" \
          f"time_interval={dt}T00:00:00Z/{dt1}T00:00:00Z' -H 'Authorization: Bearer {token}' -H 'Content-Length: 0'"
    job_ids[dt] = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)

job_status = dict()
for dt in dt_range:
    print(dt)
    job_id = json.loads(job_ids[dt].stdout)['job_id']
    cmd = f"curl https://api.airsafe.spire.com/archive/job?job_id={job_id} -H 'Authorization: Bearer {token}'"
    job_status[dt] = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)

cur_status = [json.loads(job.stdout)['job_state'] for dt, job in job_status.items()]
cur_status
set(cur_status)

download_status = dict()
for dt in dt_range:
    print(dt)
    urls = json.loads(job_status[dt].stdout)['download_urls']
    stat = list()
    for i, url in enumerate(urls):
        target_file = f'Spire_{dt}_{i}.csv'
        cmd = ['curl', '--compressed', '-o', target_file, url]
        stat.append(subprocess.run(cmd, stdout=subprocess.PIPE))
        stat.append(subprocess.run(['bzip2', target_file], stdout=subprocess.PIPE))
        bz_file = f'{target_file}.bz2'
        s3_cmd = ['aws', 's3', 'cp', bz_file, 's3://sita-coe-ds-storage-spire']
        stat.append(subprocess.run(s3_cmd, stdout=subprocess.PIPE))
        os.remove(bz_file)
    download_status[dt] = stat
