from __future__ import division
import dropbox
import sys
import os
import subprocess
from time import time, localtime, strftime
import MySQLdb
import json


access_token = 'xxxxxxxxxxxxxxxxx'

db_config = {
    'user': 'xxxx',
    'pwd' : 'xxxxxxxxxxx',
    'db'  : 'xxxxxx'
}

checksum_tmp = 'checksum.tmp'

def upload(filepath):
    client = dropbox.client.DropboxClient(access_token)

    is_exist = False
    for c in client.metadata('/')['contents']:
        if c['is_dir'] and c['path'] == '/'+db_config['db']:
            is_exist = True
    try_times = 0
    if not is_exist:
        try:
            client.file_create_folder('/ghost')
        except dropbox.rest.ErrorResponse, e:
            if e.status == 403:
                sys.stderr.write('Folder %s already exists.' % '/ghost')
                sys.stderr.flush()
            else:
                if try_times > 2:
                    sys.exit(1)
                try_times += 1

    with open(filepath, 'rb') as f:
        size = os.path.getsize(filepath)
        uploader = client.get_chunked_uploader(f, size)

        try_times = 0
        while uploader.offset < size:
            try:
                uploader.upload_chunked()
                sys.stdout.write("%f%%\r" % (uploader.offset/size,))
                sys.stdout.flush()
            except dropbox.rest.ErrorResponse, e:
                if try_times > 2:
                    sys.exit(1)
                try_times += 1
                # perform error handling and retry logic
        uploader.finish('/ghost/'+ filepath)

def dump_database(user, pwd, db, out):
    print (user, pwd, db, out)
    return subprocess.call("mysqldump -u%s -p%s %s > %s" % (user, pwd, db, out), shell=True)

def get_last_checksum():
    try:
        with open(checksum_tmp, 'r') as f:
            checksum = json.load(f)
    except IOError, e:
        sys.stderr.write(e.message)
        checksum = None
    return checksum

def is_update(user, pwd, db):
    conn=MySQLdb.connect(host="localhost",user=user, passwd=pwd, db=db, charset="utf8")
    cursor = conn.cursor()
    n = cursor.execute("show tables")
    tables = [row[0] for row in cursor.fetchall()]

    checksum = {}
    for table in tables:
        cursor.execute("checksum table %s" % (table, ))
        checksum[table] = cursor.fetchone()[1]

    last_checksum = get_last_checksum()
    if checksum != last_checksum:
        with open(checksum_tmp, 'w') as f:
            json.dump(checksum, f)
        return True
    return False

def backup():
    timestr = strftime("%Y-%m-%d_%H_%M_%S", localtime(time()))
    out_file = "%s-%s.sql" % (timestr, db_config['db'])
    err_file = timestr + "-err.log"

    try:
        flag = is_update(**db_config)
        if flag:
            dump_database(out=out_file, **db_config)
    except Exception, e:
        with open(err_file, 'w') as f:
            f.write(str(e))
        upload(err_file)
    else:
        if flag:
            upload(out_file)
            os.remove(out_file)

if __name__ == '__main__':
    backup()
