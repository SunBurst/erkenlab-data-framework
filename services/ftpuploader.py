#!/usr/bin/env
# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import ftplib
import logging.config
import ntpath
import time

from campbellsciparser import cr

from services import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/ftpuploader.yaml')
FTP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/ftpsettings.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger = logging.getLogger('ftpuploader')

ftp_cfg = utils.load_config(FTP_CONFIG_PATH)

ftpsettings = ftp_cfg['settings']
ftpserver = ftpsettings['ftp-address']
username = ftpsettings.get('username')
password = ftpsettings.get('password')

ftplogging = ftp_cfg['logging']
debuglevel = ftplogging['debuglevel']

if username and password:
    session = ftplib.FTP(ftpserver, username, password)
else:
    session = ftplib.FTP(ftpserver)

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
        with open(file_path, 'rb') as f:     # File to send.
            session.storbinary('STOR ' + file_name, f)    # Send the file.
    except IndexError as e:
        print(e)


def process_file(cfg, output_dir, site, location, file, file_info):
    name = file_info.get('name', file)
    file_path = file_info.get('file_path')
    line_num = file_info.get('line_num')
    header_row = file_info.get('header_row')

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension
    logging.info("Processing file: {file}".format(file=file))

    data = cr.read_table_data(
        infile_path=file_path,
        header_row=header_row,
        first_line_num=line_num,
    )

    num_of_new_rows = 0
    num_of_new_rows += len(data)

    logger.info("Found {num} new rows".format(num=num_of_new_rows))

    if num_of_new_rows == 0:
        logger.info("No work to be done for table: {table}".format(table=name))
        return None
    else:
        file_name = name + file_ext
        output_file_path = os.path.join(
            os.path.abspath(output_dir), site, location, file_name)

        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        cr.export_to_csv(
            data=data,
            outfile_path=output_file_path,
            export_header=True
        )

        new_line_num = line_num + num_of_new_rows
        cfg['sites'][site]['locations'][location]['files'][file]['line_num'] = new_line_num

        return output_file_path


def process_sites(cfg, args):
    """Unpacks data from the configuration file, calls the core function and updates line
        number information if tracking is enabled.

    Parameters
    ----------
    cfg : dict
        Program's configuration file.
    args : Namespace
        Arguments passed by the user. Includes site, location and file information.

    """
    try:
        output_dir = cfg['settings']['data_output_dir']
    except KeyError:
        output_dir = os.path.expanduser("~")
        msg = "No output directory set! "
        msg += "Files will be output to the user's default directory at {output_dir}"
        msg = msg.format(output_dir=output_dir)
        logger.info(msg)

    logger.debug("Output directory: {dir}".format(dir=output_dir))
    logger.debug("Getting configured sites.")

    sites = cfg['sites']

    configured_sites_msg = ', '.join("{site}".format(site=site) for site in sites)
    logger.debug("Configured sites: {sites}.".format(sites=configured_sites_msg))

    root_dir = session.pwd()

    try:
        if args.site:
            # Process specific site
            logger.info("Processing site: {site}".format(site=args.site))
            site_info = sites[args.site]
            logger.debug("Getting configured locations.")
            locations = site_info['locations']
            configured_locations_msg = ', '.join("{location}".format(
                location=location) for location in locations)
            logger.debug("Configured locations: {locations}.".format(
                locations=configured_locations_msg))
            cd_tree(args.site)
            site_dir = session.pwd()
            if args.location:
                # Process specific location
                logger.info("Processing location: {location}".format(location=args.location))
                location_info = locations[args.location]
                files = location_info['files']
                configured_files_msg = ', '.join("{file}".format(
                    file=file) for file in files)
                logger.debug("Configured files: {files}.".format(
                    files=configured_files_msg))
                cd_tree(args.location)
                location_dir = session.pwd()
                if args.file:
                    # Process specific file
                    file_info = files[args.file]
                    cd_tree(args.file)
                    outfile_path = process_file(
                        cfg,
                        output_dir,
                        args.site,
                        args.location,
                        args.file,
                        file_info)
                    if outfile_path:
                        transfer_file(outfile_path)
                else:
                    # Process all files
                    for file, file_info in files.items():
                        cd_tree(location_dir)
                        cd_tree(file)
                        outfile_path = process_file(
                            cfg,
                            output_dir,
                            args.site,
                            args.location,
                            file,
                            file_info)
                        if outfile_path:
                            transfer_file(outfile_path)
            else:
                # Process all locations
                for location, location_info in locations.items():
                    files = location_info['files']
                    cd_tree(site_dir)
                    cd_tree(location)
                    location_dir = session.pwd()
                    for file, file_info in files.items():
                        cd_tree(location_dir)
                        cd_tree(file)
                        outfile_path = process_file(
                            cfg,
                            output_dir,
                            args.site,
                            location,
                            file,
                            file_info)
                        if outfile_path:
                            transfer_file(outfile_path)
        else:
            # Process all sites
            for site, site_info in sites.items():
                locations = site_info['locations']
                cd_tree(root_dir)
                cd_tree(site)
                site_dir = session.pwd()
                for location, location_info in locations.items():
                    files = location_info['files']
                    cd_tree(site_dir)
                    cd_tree(location)
                    location_dir = session.pwd()
                    for file, file_info in files.items():
                        cd_tree(location_dir)
                        cd_tree(file)
                        outfile_path = process_file(
                            cfg,
                            output_dir,
                            site,
                            location,
                            file,
                            file_info)
                        if outfile_path:
                            transfer_file(outfile_path)
    except Exception as e:
        print(e)
    else:
        utils.save_config(APP_CONFIG_PATH, cfg)
    finally:
        session.quit()


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(
        prog='FTPUploader',
        description='Uploads datalogger files to FTP server.'
    )

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to upload.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to upload.')
    parser.add_argument('-f', '--file', action='store', required=False,
                        dest='file', help='Specific file to upload.')

    args = parser.parse_args()
    logger.debug("Arguments passed by user")
    args_msg = ', '.join("{arg}: {value}".format(
        arg=arg, value=value) for (arg, value) in vars(args).items())

    logger.debug(args_msg)

    if args.file:
        if not args.location and not args.site:
            parser.error("--site and --location are required.")
    else:
        if args.location and not args.site:
                parser.error("--site is required.")

    app_cfg = utils.load_config(APP_CONFIG_PATH)

    system_is_active = app_cfg['settings']['active']
    if not system_is_active:
        logger.info("System is not active.")
        return

    logger.info("System is active")
    logger.info("Initializing")

    start = time.time()
    process_sites(app_cfg, args)
    stop = time.time()
    elapsed = (stop - start)

    logger.info("Finished job in {elapsed} seconds".format(elapsed=elapsed))

if __name__ == '__main__':
    setup_parser()
    logger.info("Exiting.")