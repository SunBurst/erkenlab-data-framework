import ftplib
import ntpath
import os

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/fileuploader.yaml')
FTP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/ftpsettings.yaml')

ftp_cfg = utils.load_config(FTP_CONFIG_PATH)

ftpsettings = ftp_cfg['settings']
ftpserver = ftpsettings['ftp-address']
username = ftpsettings['username']
password = ftpsettings['password']

ftplogging = ftp_cfg['logging']
debuglevel = ftplogging['debuglevel']

session = ftplib.FTP(ftpserver, username, password)
session.set_debuglevel(debuglevel)


def cd_tree(current_dir):
    if current_dir != "":
        try:
            session.cwd(current_dir)
        except ftplib.error_perm:
            cd_tree("/".join(current_dir.split("/")[:-1]))
            session.mkd(current_dir)
            session.cwd(current_dir)


def transfer_files(session, file_path, site, location):

    try:
        file_name = ntpath.split(file_path)[1]
        file = open(file_path,'rb')    # File to send.
        session.storbinary('STOR ' + file_name, file)    # Send the file.
        file.close()    # Close file and FTP.
    except IndexError as e:
        print(e)


def run(**kwargs):
    cfg = utils.load_config(CONFIG_PATH)
    sites = cfg['sites']

    if not kwargs:
        for site, site_data in sites.items():
            cd_tree(site)
            for location, location_data in site_data.items():
                cd_tree(location)
                for frequency, frequency_data in location_data.items():
                    transfer_files(
                        session, frequency_data.get('file_path'), site, location)

    if 'site' in kwargs:
        site = kwargs.get('site')
        site_data = cfg['sites'][site]
        cd_tree(site)
        if not 'location' in kwargs:
            for location, location_data in site_data.items():
                cd_tree(location)
                for frequency, frequency_data in location_data.items():
                    transfer_files(
                        session, frequency_data.get('file_path'), site, location)
        else:
            location = kwargs.get('location')
            location_data = cfg['sites'][site][location]
            cd_tree(location)
            if not 'frequency' in kwargs:
                for frequency, frequency_data in location_data.items():
                    transfer_files(
                        session, frequency_data.get('file_path'), site, location)
            else:
                frequency = kwargs.get('frequency')
                frequency_data = cfg['sites'][site][location][frequency]
                transfer_files(
                        session, frequency_data.get('file_path'), site, location)

    session.quit()

if __name__=='__main__':
    run()