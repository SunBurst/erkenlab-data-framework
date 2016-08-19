#!/usr/bin/env
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import ftplib
import ntpath

from datetime import datetime

import pandas

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/ftpuploader.yaml')
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


def transfer_file(file_path):
    try:
        file_name = ntpath.split(file_path)[1]
        file = open(file_path,'rb')    # File to send.
        session.storbinary('STOR ' + file_name, file)    # Send the file.
        file.close()    # Close file and FTP.
    except IndexError as e:
        print(e)


def process_file(cfg, output_dir, site, location, log, file_id, file_data):
    file_name = file_data.get('name')
    file_path = file_data.get('file_path')
    skip_rows = file_data.get('skip_rows')
    header_row = file_data.get('header_row')

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension, e.g. '.dat', '.csv' etc.
    print("Processing file: {0}, {1}".format(file_id, file_path))

    df = pandas.read_csv(file_path, header=header_row, skiprows=range(1, (skip_rows + 1)))
    if len(df) > 0:
        header = list(df.columns.values)
        fixed_file = os.path.join(
            os.path.abspath(output_dir), site, location, log,
            file_name + " {0}".format((datetime.utcnow()).strftime("%Y-%m-%d %H %M %S %f")[:-3]) + file_ext) # Construct absolute file path to subfile.

        os.makedirs(os.path.dirname(fixed_file), exist_ok=True)    # Create file if it doesn't already exists.

        df.to_csv(fixed_file, mode='w', header=header, date_format="%Y-%m-%d %H:%M:%S%z",
                  float_format='%.3f', index=False)

        num_of_processed_rows = len(df)
        cfg['sites'][site]['locations'][location]['logs'][log]['files'][file_id]['skip_rows'] = skip_rows + num_of_processed_rows

        return fixed_file

    return None


def process_files(args):
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    sites = cfg['sites']

    if args.site:
        site_data = sites.get(args.site)
        locations = site_data.get('locations')
        cd_tree(args.site)
        if args.location:
            location_data = locations.get(args.location)
            logs = location_data.get('logs')
            cd_tree(args.location)
            if args.log:
                log_data = logs.get(args.log)
                files = log_data.get('files')
                cd_tree(args.log)
                if args.file:
                    file_data = files.get(args.file)
                    file_path = process_file(cfg, output_dir, args.site, args.location, args.log, args.file, file_data)
                    #if file_path:
                        #transfer_file(file_path)
                else:
                    for f, file_data in files.items():
                        file_path = process_file(cfg, output_dir, args.site, args.location, args.log, f, file_data)
                        if file_path:
                            transfer_file(file_path)
            else:
                for lo, log_data in logs.items():
                    files = log_data.get('files')
                    cd_tree(lo)
                    for f, file_data in files.items():
                        file_path = process_file(cfg, output_dir, args.site, args.location, lo, f, file_data)
                        if file_path:
                            transfer_file(file_path)
        else:
            for l, location_data in locations.items():
                logs = location_data.get('logs')
                cd_tree(l)
                for lo, log_data in logs.items():
                    files = log_data.get('files')
                    cd_tree(lo)
                    for f, file_data in files.items():
                        file_path = process_file(cfg, output_dir, args.site, l, lo, f, file_data)
                        if file_path:
                            transfer_file(file_path)
    else:
        for s, site_data in sites.items():
            locations = site_data.get('locations')
            cd_tree(s)
            for l, location_data in locations.items():
                logs = location_data.get('logs')
                cd_tree(l)
                for lo, log_data in logs.items():
                    files = log_data.get('files')
                    cd_tree(lo)
                    for f, file_data in files.items():
                        file_path = process_file(cfg, output_dir, s, l, lo, f, file_data)
                        if file_path:
                            transfer_file(file_path)

    session.quit()
    utils.save_config(CONFIG_PATH, cfg)


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='FTPUploader',
                                     description='Uploads files to FTP server.')

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to upload.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to upload.')
    parser.add_argument('-i', '--log', action='store', required=False,
                        dest='log', help='Specific log to upload.')
    parser.add_argument('-f', '--file', action='store', required=False,
                        dest='file', help='Specific file to upload.')

    args = parser.parse_args()

    if args.file:
        if not args.log:
            parser.error("--site, --location and --log are required.")
        if not args.location:
            parser.error("--site, --location and --log are required.")
        if not args.site:
            parser.error("--site, --location and --log are required.")
    else:
        if args.log:
            if not args.location:
                parser.error("--site and --location are required.")
            if not args.site:
                    parser.error("--site and --location are required.")
        else:
            if args.location:
                if not args.site:
                    parser.error("--site is required.")

    process_files(args)

if __name__=='__main__':
    setup_parser()