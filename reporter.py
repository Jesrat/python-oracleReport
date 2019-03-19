import os
import csv
import logging
import tarfile
from datetime import datetime


from pyutils.remote import Sftp
from pyutils.oracle import OraConn
from pyutils.localenv import localenv

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                   CONFIGURATIONS HERE!!
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# for each item in reports[] like: reports = ['item', 'anotheritem']
# it will search a sql file called 'item.sql' and 'anotheritem.sql' inside SQL_DIR
# and it will generate a report for each one called rprt_item20190101.csv and rprt_anotheritem20190101.csv
# then it will create a tar.gz file for each csv rprt_item20190101.tar.gz and rprt_anotheritem20190101.tar.gz
# and delete the csv files from the working dir
# finally it will upload to an sftp server

reports = [
    'sv'
]

localenv.load()
HOME_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(HOME_DIR, 'sql')
REPORT_DIR = os.path.join(HOME_DIR, 'reports')
DESTINATION_HOME = os.path.join('.', 'reports')

ORA_CONN = localenv.get('ORA_CONN')
ORA_MODULE_NAME = 'Report Generator Do Not Kill!!'

FTP_REMOTE_HOST = localenv.get('FTP_REMOTE_HOST')
FTP_REMOTE_PORT = localenv.get('FTP_REMOTE_PORT', cast=int)
FTP_REMOTE_USER = localenv.get('FTP_REMOTE_USER')
FTP_REMOTE_PASS = localenv.get('FTP_REMOTE_PASS')
sftp_auth = (FTP_REMOTE_HOST, FTP_REMOTE_PORT, FTP_REMOTE_USER, FTP_REMOTE_PASS)

LOG_FILE = os.path.join(HOME_DIR, 'reporter.log')
logging.basicConfig(filename=LOG_FILE, filemode='a', format='%(asctime)s %(module)s %(message)s', level=logging.INFO)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def progress(message):
    logging.info(message)


def main():
    # first of all we place our self in reports dir
    # as working directory
    os.chdir(REPORT_DIR)
    for report in reports:
        try:
            file_name = f'rprt_{report}{datetime.now():%Y%m%d}'
            csv_file = f'{file_name}.csv'
            tar_file = f'{file_name}.tar.gz'
            destination = os.path.join(DESTINATION_HOME, tar_file)

            progress('reading sql file')
            with open(os.path.join(SQL_DIR, f'{report}.sql')) as f:
                query = f.read()
            progress(f'got query: \n{query}')

            progress('starting oracle conn...')
            with OraConn(ORA_CONN, ret="cursor", module_name=ORA_MODULE_NAME) as cursor:
                cursor.arraysize = 10000
                # working!
                progress(f'i got oracle conn {cursor}\n lets query...')
                cursor.execute(query)
                progress('query has been executed!')
                headers = [i[0] for i in cursor.description]
                progress('fetching rows by and saving to csv file...')
                with open(csv_file, 'w') as csvfile:
                    file = csv.writer(csvfile)
                    file.writerow(headers)
                    file.writerows(cursor)

            progress('compressing csv file...')
            with tarfile.open(tar_file, "w:gz") as tar:
                tar.add(csv_file)
            progress('csv file compressed!')
            progress('removing csv file...')
            os.remove(csv_file)
            progress('csv file removed!')

            # create sftp conn to remote server after report is generated
            # cause query may delay to much and if we create conn before
            # sftp conn may get closed
            progress(f'starting sft conn to {FTP_REMOTE_HOST}...')
            with Sftp(sftp_auth) as sftp:
                progress(f'uploading file to {destination}')
                sftp.put(tar_file, destination)

            progress('report generated and delivered successfully')

        except Exception as e:
            logging.exception(f'Error in reporter {e}')


if __name__ == "__main__":
        main()

