import schedule
import time
import os

def dump_table_names(sqlrunner_base, base_path):
    command = "echo 'show tables'| {sqlrunner_base} - |cut -d, -f3 > {base_path}/tables.txt"
    os.system(command)

def dump_table_contents(sqlrunner_base, base_path):
    #For each table in DBNAME.tables.DATE.txt, dump the data to a csv file
    command = "cat {basepath}/tables.txt | while read t; do \
        echo 'select * from $t' | {sqlrunner_base} delim=',' - | gzip -c > {basepath}/$t.csv.gz \
    done"
    os.system(command)

def compress_dump(base_path):
    command = "tar -zcvf {base_path}.tar.gz {base_path}/"
    os.system(command)

def encrypt_dump(base_path):
    command = "cat {base_path}.tar.gz | gpg --output {base_path}.gpg --encrypt --recipient db-backup"
    os.system(command)

def job(db, dbname, user, password):
    date = datetime.now()
    base_path = "backups/{date}"
    base_path_with_dbname = "{base_path}/{dbname}"
    try:
        os.makedirs(base_path_with_dbname)
    except e:
        pass
    sqlrunner_base = "sqlrunner db={db} dbname={dbname} user={user} pass={password}"
    dump_table_names(sqlrunner_base, base_path_with_dbname)
    dump_table_contents(sqlrunner_base, base_path_with_dbname)
    compress_dump(base_path)
    encrypt_dump(base_path)

def generate_keys():
    command = "gpg --quick-generate-key db-backup rsa"
    os.system(command)
    
def main():
    parser = argparse.ArgumentParser(description='Create an encrypted backup of your database every day')
    parser.add_argument('-db', '--database', metavar="database", type=str,
                        help='database to backup')
    parser.add_argument('-n', '--dbname', metavar="database name", type=str,
                        help='database name')
    parser.add_argument('-u', '--username', metavar="database username", type=str,
                        help='database username')
    parser.add_argument('-p', '--password', metavar="database password", type=str,
                        help='database password')
    try:
        os.makedirs("backups")
    except e:
        pass
    generate_keys()
    j = lambda : job(args.db, args.dbname, args.username, args.password)
    schedule.every().day.do(j)
    while True:
        schedule.run_pending()
        time.sleep(1)
