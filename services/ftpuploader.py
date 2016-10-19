#!/usr/bin/env
# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import ftplib
import ntpath

from campbellsciparser import devices

from services import utils

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


def process_log(cfg, output_dir, site, location, log, log_info):
    name = log_info.get('name', log)
    file_path = log_info.get('file_path')
    line_num = log_info.get('line_num')
    header_row = log_info.get('header_row')
    include_time_zone = log_info.get('include_time_zone')

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension
    msg = "Processing site: {site}, location: {location}, log: {log}, file {file}"
    msg = msg.format(site=site, location=location, log=log, file=file_path)
    print(msg)

    baseparser = devices.CampbellSCIBaseParser()
    data = baseparser.read_data(
        infile_path=file_path,
        header_row=header_row,
        line_num=line_num
    )

    if len(data) > 0:

        file_name = name + file_ext
        output_file_path = os.path.join(
            os.path.abspath(output_dir), site, location, log, file_name)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        baseparser.export_to_csv(
            data=data,
            outfile_path=output_file_path,
            export_headers=True,
            include_time_zone=include_time_zone
        )

        num_of_processed_rows = len(data)
        new_line_num = line_num + num_of_processed_rows
        cfg['sites'][site]['locations'][location]['logs'][log]['line_num'] = new_line_num

        return output_file_path

    return None


def process_files(args):
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    sites = cfg['sites']
    root_dir = session.pwd()

    try:
        if args.site:
            site_info = sites.get(args.site)
            locations = site_info.get('locations')
            cd_tree(args.site)
            site_dir = session.pwd()
            if args.location:
                location_info = locations.get(args.location)
                logs = location_info.get('logs')
                cd_tree(args.location)
                location_dir = session.pwd()
                if args.log:
                    log_info = logs.get(args.log)
                    cd_tree(args.log)
                    outfile_path = process_log(
                        cfg,
                        output_dir,
                        args.site,
                        args.location,
                        args.log,
                        log_info)
                    if outfile_path:
                        transfer_file(outfile_path)
                else:
                    for log, log_info in logs.items():
                        cd_tree(location_dir)
                        cd_tree(log)
                        outfile_path = process_log(
                            cfg,
                            output_dir,
                            args.site,
                            args.location,
                            log,
                            log_info)
                        if outfile_path:
                            transfer_file(outfile_path)
            else:
                for location, location_info in locations.items():
                    logs = location_info.get('logs')
                    cd_tree(site_dir)
                    cd_tree(location)
                    location_dir = session.pwd()
                    for log, log_info in logs.items():
                        cd_tree(location_dir)
                        cd_tree(log)
                        outfile_path = process_log(
                            cfg,
                            output_dir,
                            args.site,
                            location,
                            log,
                            log_info)
                        if outfile_path:
                            transfer_file(outfile_path)
        else:
            for site, site_info in sites.items():
                locations = site_info.get('locations')
                cd_tree(root_dir)
                cd_tree(site)
                site_dir = session.pwd()
                for location, location_info in locations.items():
                    logs = location_info.get('logs')
                    cd_tree(site_dir)
                    cd_tree(location)
                    location_dir = session.pwd()
                    for log, log_info in logs.items():
                        cd_tree(location_dir)
                        cd_tree(log)
                        outfile_path = process_log(
                            cfg,
                            output_dir,
                            site,
                            location,
                            log,
                            log_info)
                        if outfile_path:
                            transfer_file(outfile_path)
    except Exception as e:
        print(e)
    else:
        utils.save_config(CONFIG_PATH, cfg)
    finally:
        session.quit()


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(
        prog='FTPUploader',
        description='Uploads exported datalogger files to FTP server.'
    )

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to upload.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to upload.')
    parser.add_argument('-i', '--log', action='store', required=False,
                        dest='log', help='Specific log to upload.')

    args = parser.parse_args()

    if args.log:
        if not args.location and not args.site:
            parser.error("--site and --location are required.")
    else:
        if args.location and not args.site:
                parser.error("--site is required.")

    process_files(args)

if __name__ == '__main__':
    setup_parser()
