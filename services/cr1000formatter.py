#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Script for parsing and exporting Campbell CR10X mixed-array files. """

import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging.config
import time

from collections import OrderedDict

from campbellsciparser import devices

from services import common, utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr1000formatter.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger = logging.getLogger('cr1000formatter')


def process_file(cfg, output_dir, site, location, file, file_info, track=False):
    """Splits apart mixed-array location files into subfiles based on each rows' array id.

    Args:
        cfg (dict): Program's configuration file.
        output_dir (string): Output directory.
        site (str): Site id.
        location (str): Location id.
        file (str):
        file_info (dict):
        track: If true, update configuration file with the last read line number.

    Returns:
        Updated configuration file.

    """
    header_row = file_info.get('header_row')
    headers = file_info.get('headers')
    export_columns = file_info.get('export_columns')
    name = file_info.get('name', file)
    convert_column_values = file_info.get('convert_data_column_values')
    file_path = file_info.get('file_path')
    line_num = file_info.get('line_num', 0)
    time_columns = file_info.get('time_columns')
    time_parsed_column_name = file_info.get('time_parsed_column_name')
    time_zone = file_info.get('time_zone')
    to_utc = file_info.get('to_utc', False)
    include_time_zone = file_info.get('include_time_zone', False)

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension

    cr1000parser = devices.CR1000Parser(time_zone)

    if headers:
        data = cr1000parser.read_data(
            infile_path=file_path,
            headers=headers,
            line_num=line_num,
            convert_time=True,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc
        )
    elif header_row:
        header_row = int(header_row)
        data = cr1000parser.read_data(
            infile_path=file_path,
            header_row=header_row,
            line_num=line_num,
            convert_time=True,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc
        )
    else:
        raise common.NoHeadersException("Headers representation not found!")

    num_of_new_rows = 0
    num_of_new_rows += len(data)

    print("Found {0} new rows".format(num_of_new_rows))
    if convert_column_values:
        data_converted_values = data
        for column_name, convert_column_info in convert_column_values.items():
            value_type = convert_column_info.get('value_type')

            if value_type == 'time':
                value_time_columns = convert_column_info.get('value_time_columns')
                data_converted_values = cr1000parser.convert_time(
                    data=data_converted_values,
                    time_parsed_column=column_name,
                    time_columns=value_time_columns,
                    replace_time_column=column_name,
                    to_utc=to_utc)
            else:
                msg = "Only time conversion is supported in this version."
                raise common.UnsupportedValueConversionType(msg)

            converted_values_data = []
            for row in data_converted_values:
                converted_values = OrderedDict()
                for conv_name, conv_value in row.items():
                    if conv_name == column_name:
                        converted_values[column_name] = conv_value
                converted_values_data.append(converted_values)
            data = [
                row for row in common.update_column_values_generator(
                    data_old=data_converted_values,
                    data_new=converted_values_data)
                ]

    data_to_export = []
    for row in data:
        data_to_export.append(OrderedDict(
            [(name, value) for name, value in row.items() if name in export_columns]))

    file_name = name + file_ext
    outfile_path = os.path.join(
        os.path.abspath(output_dir), site, location, name, file_name)
    cr1000parser.export_to_csv(
        data=data_to_export,
        outfile_path=outfile_path,
        export_headers=True,
        include_time_zone=include_time_zone)

    if track:
        if num_of_new_rows > 0:
            new_line_num = line_num + num_of_new_rows
            cfg['sites'][site]['locations'][location]['line_num'] = new_line_num

    print("Done processing site {0}, location {1}".format(site, location))

    return cfg


def process_sites(cfg, args):
    """Unpacks data from the configuration file, calls the core function and updates
        line number information if tracking is enabled.

    Parameters
    ----------
    cfg : dict
        Program's configuration file.
    args : Namespace
        Arguments passed by the user. Includes site, location, table-based file and
        tracking information.

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

    if args.track:
        logger.info("Tracking is enabled.")
    else:
        logger.info("Tracking is disabled.")

    if args.site:
        logger.info("Processing site: {site}".format(site=args.site))
        site_info = sites[args.site]
        logger.debug("Getting configured locations.")
        locations = site_info['locations']
        configured_locations_msg = ', '.join("{location}".format(
            location=location) for location in locations)
        logger.debug("Configured locations: {locations}.".format(
            locations=configured_locations_msg))
        if args.location:
            location_info = locations[args.location]
            logger.debug("Getting configured files.")
            files = location_info['files']
            configured_files_msg = ', '.join("{file}".format(
                file=file) for file in files)
            logger.debug("Configured files: {files}.".format(
                files=configured_files_msg))
            if args.file:
                file_info = files[args.file]
                cfg = process_file(
                    cfg=cfg,
                    output_dir=output_dir,
                    site=args.site,
                    location=args.location,
                    file=args.file,
                    file_info=file_info,
                    track=False)
            else:
                for file, file_info in files.items():
                    cfg = process_file(
                        cfg=cfg,
                        output_dir=output_dir,
                        site=args.site,
                        location=args.location,
                        file=file,
                        file_info=file_info,
                        track=False)

            logger.info("Done processing location: {location}".format(location=args.location))
        else:
            for location, location_info in locations.items():
                files = location_info['files']
                configured_files_msg = ', '.join("{file}".format(
                    file=file) for file in files)
                logger.debug("Configured files: {files}.".format(
                    files=configured_files_msg))
                for file, file_info in files.items():
                    cfg = process_file(
                        cfg=cfg,
                        output_dir=output_dir,
                        site=args.site,
                        location=location,
                        file=file,
                        file_info=file_info,
                        track=False)

                logger.info(
                    "Done processing location: {location}".format(location=location))

        logger.info("Done processing site: {site}".format(site=args.site))
    else:
        for site, site_info in sites.items():
            logger.info("Processing site: {site}".format(site=site))
            locations = site_info['locations']
            configured_locations_msg = ', '.join("{location}".format(
                location=location) for location in locations)
            logger.debug("Configured locations: {locations}.".format(
                locations=configured_locations_msg))
            for location, location_info in locations.items():
                files = location_info['files']
                configured_files_msg = ', '.join("{file}".format(
                    file=file) for file in files)
                logger.debug("Configured files: {files}.".format(
                    files=configured_files_msg))
                for file, file_info in files.items():
                    cfg = process_file(
                        cfg=cfg,
                        output_dir=output_dir,
                        site=site,
                        location=location,
                        file=file,
                        file_info=file_info,
                        track=False)

                logger.info("Done processing location: {location}".format(location=location))

            logger.info("Done processing site: {site}".format(site=site))

    if args.track:
        logger.info("Updating config file.")
        utils.save_config(APP_CONFIG_PATH, cfg)


def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='CR1000Formatter',
        description='Program for formatting and exporting Campbell CR1000 mixed array datalogger files.'
    )
    parser.add_argument('-s', '--site', action='store', dest='site',
                        help='Site to process.')
    parser.add_argument('-l', '--location', action='store', dest='location',
                        help='Location to process.')
    parser.add_argument('-f', '--file', action='store', dest='file',
                        help='Table-based file to process.')
    parser.add_argument(
        '-t', '--track',
        help='Track file line number.',
        dest='track',
        action='store_true',
        default=False
    )

    args = parser.parse_args()

    logger.debug("Arguments passed by user")
    args_msg = ', '.join("{arg}: {value}".format(
        arg=arg, value=value) for (arg, value) in vars(args).items())

    logger.debug(args_msg)

    app_cfg = utils.load_config(APP_CONFIG_PATH)

    if args.file:
        if not args.location and not args.site:
            parser.error("--site and --location are required.")
    if args.location and not args.site:
        parser.error("--site is required.")

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
    main()
    logger.info("Exiting.")
