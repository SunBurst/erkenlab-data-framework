#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for formatting and exporting Campbell CR10X mixed-array datalogger files. """

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import logging
import logging.config
import time

from campbellsciparser import cr

from services import common
from services import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr10xformatter.yaml')
LOGGING_CONFIG_PATH = utils.load_config(os.path.join(BASE_DIR, 'cfg/logging.yaml'))

logging.config.dictConfig(LOGGING_CONFIG_PATH)
logger = logging.getLogger('simpleExample')


def process_array_ids(site, location, array_ids_data, time_zone, time_format_args_library,
                      output_dir, array_ids_info, file_ext):
    """Splits apart mixed array location files into subfiles based on each rows' array id.

    Parameters
    ----------
    site : str
        Site id.
    location : str
        Location id.
    array_ids_data : dict of DataSet
        Mixed array data set, split by array ids.
    time_zone : str
        String representation of a valid pytz time zone. (See pytz docs
        for a list of valid time zones). The time zone refers to collected data's
        time zone, which defaults to UTC and is used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string format columns sequence to match against
        when parsing time values.
    output_dir : str
        Output directory.
    array_ids_info : dict of dict
        File processing and exporting information.
    file_ext : str
        Output file extension.

    Raises
    ------
    UnsupportedValueConversionType: If an unsupported data value conversion type is given.

    """

    for array_id, array_id_info in array_ids_info.items():
        array_name = array_id_info.get('name', array_id)

        logging.info("Processing array: {array_name}".format(array_name=array_name))
        array_id_data = array_ids_data.get(array_name)
        logging.info("{num} new rows".format(num=len(array_id_data)))

        if not array_id_data:
            logging.info("No work to be done for array: {array_name}".format(array_name=array_name))
            continue

        column_names = array_id_info.get('column_names')
        export_columns = array_id_info.get('export_columns')
        include_time_zone = array_id_info.get('include_time_zone', False)
        time_columns = array_id_info.get('time_columns')
        time_parsed_column_name = array_id_info.get('time_parsed_column_name', 'Timestamp')
        to_utc = array_id_info.get('to_utc', False)
        convert_column_values = array_id_info.get('convert_column_values')

        array_id_file = array_name + file_ext
        array_id_file_path = os.path.join(os.path.abspath(output_dir), site, location, array_id_file)
        array_id_mismatches_file = array_name + ' Mismatches' + file_ext
        array_id_mismatches_file_path = os.path.join(
            os.path.abspath(output_dir), site, location, array_id_mismatches_file)

        array_id_data_with_column_names, mismatches = cr.update_column_names(
            data=array_id_data,
            column_names=column_names,
            match_row_lengths=True,
            get_mismatched_row_lengths=True)

        if convert_column_values:
            array_id_data_backup = cr.DataSet(
                [cr.Row([(name, value) for name, value in row.items()])
                 for row in array_id_data_with_column_names]
            )

            for column_name, convert_column_info in convert_column_values.items():
                value_type = convert_column_info.get('value_type')

                if value_type == 'time':
                    value_time_columns = convert_column_info.get('value_time_columns')
                    array_id_data_converted_values_all = cr.parse_time(
                        data=array_id_data_with_column_names,
                        time_zone=time_zone,
                        time_format_args_library=time_format_args_library,
                        time_parsed_column=column_name,
                        time_columns=value_time_columns,
                        replace_time_column=column_name,
                        to_utc=to_utc)
                else:
                    msg = "Only time conversion is supported in this version."
                    raise common.UnsupportedValueConversionType(msg)

                converted_values_data = []

                for row in array_id_data_converted_values_all:
                    converted_values = cr.Row()
                    for converted_name, converted_value in row.items():
                        if converted_name == column_name:
                            converted_values[column_name] = converted_value

                    converted_values_data.append(converted_values)

                array_id_data_with_column_names = [
                    row for row in common.update_column_values_generator(
                        data_old=array_id_data_backup,
                        data_new=converted_values_data)
                ]

        array_id_data_time_converted = cr.parse_time(
            data=array_id_data_with_column_names,
            time_zone=time_zone,
            time_format_args_library=time_format_args_library,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc)

        data_to_export = cr.DataSet()
        for row in array_id_data_time_converted:
            data_to_export.append(cr.Row(
                [(name, value) for name, value in row.items() if name in export_columns]
            ))

        cr.export_to_csv(
            data=data_to_export,
            outfile_path=array_id_file_path,
            export_header=True,
            include_time_zone=include_time_zone
        )

        if mismatches:
            cr.export_to_csv(data=mismatches, outfile_path=array_id_mismatches_file_path)


def process_location(cfg, output_dir, site, location, location_info, track=False):
    """Splits apart mixed array location files into subfiles based on each rows' array id.

    Parameters
    ----------
    cfg : dict
        Program's configuration file.
    output_dir : str
        Output directory.
    site : str
        Site id.
    location : str
        Location id.
    location_info : dict
        Location information including the location's array ids lookup table, source file
        path and last read line number.
    track: If true, update configuration file with the last read line number.

    Returns
    -------
        Updated configuration file.

    """
    logging.info("Processing location: {location}".format(location=location))
    logging.debug("Getting location configuration.")
    array_ids_info = location_info.get('array_ids', {})
    file_path = location_info.get('file_path')
    line_num = location_info.get('line_num', 0)
    time_zone = location_info.get('time_zone')
    time_format_args_library = location_info.get('time_format_args_library')
    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension

    array_id_names = {
        array_id: array_id_info.get('name', array_id)
        for array_id, array_id_info in array_ids_info.items()
    }

    data = cr.read_array_ids_data(
        infile_path=file_path,
        first_line_num=line_num,
        fix_floats=True,
        array_id_names=array_id_names
    )

    num_of_new_rows = 0

    for array_id, array_id_data in data.items():
        num_of_new_rows += len(array_id_data)

    logging.info("Found {num} new rows".format(num=num_of_new_rows))

    process_array_ids(
        site=site,
        location=location,
        array_ids_data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        output_dir=output_dir,
        array_ids_info=array_ids_info,
        file_ext=file_ext
    )

    if track:
        if num_of_new_rows > 0:
            cfg['sites'][site]['locations'][location]['line_num'] = line_num + num_of_new_rows

    msg = "Done processing site {site}, location {location}"
    logging.info(msg.format(site=site, location=location))

    return cfg


def process_sites(cfg, args):
    """Unpacks data from the configuration file, calls the core function and updates line
        number information if tracking is enabled.

    Parameters
    ----------
    cfg : dict
        Program's configuration file.
    args : Namespace
        Arguments passed by the user. Includes site, location and tracking information.

    """
    try:
        output_dir = cfg['settings']['data_output_dir']
    except KeyError:
        output_dir = os.path.expanduser("~")
        msg = "No output directory set! "
        msg += "Files will be output to the user's default directory at {output_dir}"
        msg = msg.format(output_dir=output_dir)
        logging.info(msg)

    logging.debug("Output directory: {dir}".format(dir=output_dir))
    logging.debug("Getting configured sites.")
    sites = cfg['sites']
    configured_sites_msg = ', '.join("{site}".format(site=site) for site in sites)
    logging.debug("Configured sites: {sites}.".format(sites=configured_sites_msg))

    if args.track:
        logging.info("Tracking is enabled.")
    else:
        logging.info("Tracking is disabled.")

    if args.site:
        logging.info("Processing site: {site}".format(site=args.site))
        site_info = sites[args.site]
        logging.debug("Getting configured locations.")
        locations = site_info['locations']
        configured_locations_msg = ', '.join("{location}".format(
            location=location) for location in locations)
        logging.debug("Configured locations: {locations}.".format(
            locations=configured_locations_msg))
        if args.location:
            location_info = locations[args.location]
            cfg = process_location(
                cfg, output_dir, args.site, args.location, location_info, args.track)
        else:
            for location, location_info in locations.items():
                cfg = process_location(
                    cfg, output_dir, args.site, location, location_info, args.track)

        logging.info("Done processing site: {site}".format(site=args.site))
    else:
        for site, site_info in sites.items():
            logging.info("Processing site: {site}".format(site=site))
            locations = site_info['locations']
            configured_locations_msg = ', '.join("{location}".format(
                location=location) for location in locations)
            logging.debug("Configured locations: {locations}.".format(
                locations=configured_locations_msg))
            for location, location_info in locations.items():
                cfg = process_location(
                    cfg, output_dir, site, location, location_info, args.track)

            logging.info("Done processing site: {site}".format(site=args.site))

    if args.track:
        logging.info("Updating config file.")
        utils.save_config(APP_CONFIG_PATH, cfg)


def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='CR10XFormatter',
        description='Program for formatting and exporting Campbell CR10X mixed array datalogger files.'
    )
    parser.add_argument('-s', '--site', action='store', dest='site',
                        help='Site to split.')
    parser.add_argument('-l', '--location', action='store', dest='location',
                        help='Location to split.')
    parser.add_argument(
        '-t', '--track',
        help='Track file line number.',
        dest='track',
        action='store_true',
        default=False
    )

    args = parser.parse_args()
    logging.debug("Arguments passed by user")
    args_msg = ', '.join("{arg}: {value}".format(
        arg=arg, value=value) for (arg, value) in vars(args).items())

    logging.debug(args_msg)

    if args.location and not args.site:
        parser.error("--site and --location are required.")

    app_cfg = utils.load_config(APP_CONFIG_PATH)

    system_is_active = app_cfg['settings']['active']
    if not system_is_active:
        logging.info("System is not active.")
        return

    logging.info("System is active")
    logging.info("Initializing")

    start = time.time()
    process_sites(app_cfg, args)
    stop = time.time()
    elapsed = (stop - start)

    logging.info("Finished job in {elapsed} seconds".format(elapsed=elapsed))

if __name__ == '__main__':
    main()
    logging.info("Exiting.")
