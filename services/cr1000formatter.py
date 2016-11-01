#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Script for parsing and exporting Campbell CR10X mixed-array files. """

import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging.config
import time

from campbellsciparser import cr

from services import common, utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr1000formatter.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger = logging.getLogger('cr1000formatter')


def convert_data_time_values(data, column_name, value_time_columns, time_zone,
                             time_format_args_library, to_utc):
    """Convert time values (data column).

    Parameters
    ----------
    data : DataSet
        Data set to convert.
    column_name: str or int
        Time column name (or index) to convert.
    value_time_columns : list of str or int
        Column(s) (names or indices) to use for time conversion.
    time_zone : str
        String representation of a valid pytz time zone. (See pytz docs
        for a list of valid time zones). The time zone refers to collected data's
        time zone, which defaults to UTC and is used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string format columns sequence to match against
        when parsing time values.
    to_utc : bool
        If the data type to convert is 'time', convert to UTC.

    Returns
    -------
    DataSet
        Data time converted data set.

    """
    return cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_parsed_column=column_name,
        time_columns=value_time_columns,
        replace_time_column=column_name,
        to_utc=to_utc)


def make_data_set_backup(data):
    """Returns a copy of the given data set.

    Parameters
    ----------
    data : DataSet
        Data set to backup.

    Returns
    -------
    DataSet
        Copy of given data set.

    """
    return cr.DataSet(
        [cr.Row([(name, value) for name, value in row.items()])
         for row in data]
    )


def make_export_data_set(data, columns_to_export):
    """Create an 'export' data set, i.e. a data set filtered by columns to export.

    Parameters
    ----------
    data : DataSet
        Data set to extract columns from.

    columns_to_export : list of str or int
        Columns to extract from source data set.

    Returns
    -------
    DataSet
        Data set ready to export.

    """
    data_to_export = cr.DataSet()
    for row in data:
        data_to_export.append(cr.Row(
            [(name, value) for name, value in row.items() if name in columns_to_export]
        ))

    return data_to_export


def restore_data_after_data_time_conversion(data, data_backup, converted_column_name):
    """Convenience for restoring time values that was removed for data time conversion.

    Parameters
    ----------
    data : DataSet
        Data time value converted data set.
    data_backup : DataSet
        Source data set.
    converted_column_name : str or int
        Column name (or index) that was converted.

    Returns
    -------
    DataSet
        Data time converted data set with its original time values restored.

    """
    data_converted = []

    for row in data:
        converted_values = cr.Row()
        for converted_name, converted_value in row.items():
            if converted_name == converted_column_name:
                converted_values[converted_column_name] = converted_value

                data_converted.append(converted_values)

    data_merged = [row for row in common.update_column_values_generator(
        data_old=data_backup,
        data_new=data_converted
    )]

    return data_merged


def convert_data_column_values(data, values_to_convert, time_zone, time_format_args_library, to_utc):
    """Converts certain column values.

    Parameters
    ----------
    data : DataSet
        data set to convert.
    values_to_convert : dict
        Columns to convert.
    time_zone : str
        String representation of a valid pytz time zone. (See pytz docs
        for a list of valid time zones). The time zone refers to collected data's
        time zone, which defaults to UTC and is used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string format columns sequence to match against
        when parsing time values.
    to_utc : bool
        If the data type to convert is 'time', convert to UTC.

    Returns
    -------
    DataSet
        Column values converted data set.

    """
    data_converted = cr.DataSet()

    data_backup = make_data_set_backup(data)

    for column_name, convert_column_info in values_to_convert.items():
        value_type = convert_column_info.get('value_type')
        value_time_columns = convert_column_info.get('value_time_columns')

        if value_type == 'time':
            array_id_data_converted_values_all = convert_data_time_values(
                data=data,
                column_name=column_name,
                value_time_columns=value_time_columns,
                time_zone=time_zone,
                time_format_args_library=time_format_args_library,
                to_utc=to_utc
            )
        else:
            msg = "Only time conversion is supported in this version."
            raise common.UnsupportedValueConversionType(msg)

        data_converted = restore_data_after_data_time_conversion(
            data=array_id_data_converted_values_all,
            data_backup=data_backup,
            converted_column_name=column_name
        )

    return data_converted


def process_file(cfg, output_dir, site, location, file, file_info, track=False):
    """Splits apart mixed-array location files into subfiles based on each rows' array id.

    Parameters
    ----------
    cfg : dict
        Program's configuration file.
    output_dir : string
        Output directory.
    site : str
        Site id.
    location : str
        Location id.
    file : str
        Table-based file id.
    file_info : dict
        Table-based file information.
    track: If true, update configuration file with the last read line number.

    Returns
    -------
        Updated configuration file.

    """
    header_row = file_info.get('header_row')
    logger.debug("Header row: {header_row}".format(header_row=header_row))

    column_names = file_info.get('column_names')
    logger.debug("Column names: {column_names}".format(column_names=column_names))

    export_columns = file_info.get('export_columns')
    logger.debug("Export columns: {export_columns}".format(export_columns=export_columns))

    name = file_info.get('name', file)
    logger.debug("Name: {name}".format(name=name))

    convert_column_values = file_info.get('convert_data_column_values')
    logger.debug("Convert column values: {convert_column_values}".format(
        convert_column_values=convert_column_values))

    file_path = file_info.get('file_path')
    logger.debug("File path: {file_path}".format(file_path=file_path))

    line_num = file_info.get('line_num', 0)
    logger.debug("Line num: {line_num}".format(line_num=line_num))

    time_columns = file_info.get('time_columns')
    logger.debug("Time columns: {time_columns}".format(time_columns=time_columns))

    time_format_args_library = file_info.get('time_format_args_library')
    logger.debug("Time format args library: {time_format_args_library}".format(
        time_format_args_library=time_format_args_library))

    time_parsed_column_name = file_info.get('time_parsed_column_name')
    logger.debug("Time parsed column name: {time_parsed_column_name}".format(
        time_parsed_column_name=time_parsed_column_name))

    time_zone = file_info.get('time_zone')
    logger.debug("Time zone: {time_zone}".format(time_zone=time_zone))

    to_utc = file_info.get('to_utc', False)
    logger.debug("To UTC: {to_utc}".format(to_utc=to_utc))

    include_time_zone = file_info.get('include_time_zone', False)
    logger.debug("Include time zone: {include_time_zone}".format(include_time_zone=include_time_zone))

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension
    logger.debug("File ext: {file_ext}".format(file_ext=file_ext))

    if column_names:
        data = cr.read_table_data(
            infile_path=file_path,
            header=column_names,
            first_line_num=line_num,
            parse_time_columns=True,
            time_zone=time_zone,
            time_format_args_library=time_format_args_library,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc
        )
    elif header_row:
        header_row = int(header_row)
        data = cr.read_table_data(
            infile_path=file_path,
            header_row=header_row,
            first_line_num=line_num,
            parse_time_columns=True,
            time_zone=time_zone,
            time_format_args_library=time_format_args_library,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc
        )
    else:
        raise common.NoHeadersException("Headers representation not found!")

    num_of_new_rows = 0
    num_of_new_rows += len(data)

    logger.info("Found {num} new rows".format(num=num_of_new_rows))
    if num_of_new_rows == 0:
        logger.info("No work to be done for file: {file}".format(file=file))
        return cfg

    if convert_column_values:
        data = convert_data_column_values(
            data=data,
            values_to_convert=convert_column_values,
            time_zone=time_zone,
            time_format_args_library=time_format_args_library,
            to_utc=to_utc
        )

    data_to_export = make_export_data_set(
        data=data, columns_to_export=export_columns)

    file_name = name + file_ext
    outfile_path = os.path.join(
        os.path.abspath(output_dir), site, location, name, file_name)

    cr.export_to_csv(
        data=data_to_export,
        outfile_path=outfile_path,
        export_header=True,
        include_time_zone=include_time_zone
    )

    if track:
        if num_of_new_rows > 0:
            new_line_num = line_num + num_of_new_rows
            logger.info("Updated up to line number {num}".format(num=new_line_num))
            cfg['sites'][site]['locations'][location]['line_num'] = new_line_num

    msg = "Done processing site {site}, location {location}"
    logger.info(msg.format(site=site, location=location))

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
