#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for formatting and exporting Campbell CR10X mixed-array datalogger files. """

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import logging.config
import time

from campbellsciparser import cr

from services import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
APP_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/loggerfilesformatter.yaml')
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/logging.yaml')

logging_conf = utils.load_config(LOGGING_CONFIG_PATH)
logging.config.dictConfig(logging_conf)
logger_info = logging.getLogger('loggerfilesformatter_info')
logger_debug = logging.getLogger('loggerfilesformatter_debug')


class NoHeadersException(ValueError):
    pass


class UnsupportedValueConversionType(ValueError):
    pass


def update_column_values_generator(data_old, data_new):
    """Iterates one old and one new data set, replacing the modified columns.

    Parameters
    ----------
    data_old : DataSet
        Data set to update.
    data_new : DataSet
        Data set to read updates from.

    Yields
    ------
    DataSet
        Updated data set.

    """
    for row_old, row_new in zip(data_old, data_new):
        for name, value_new in row_new.items():
            row_old[name] = value_new
        yield row_old


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

    data_merged = [row for row in update_column_values_generator(
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
            raise UnsupportedValueConversionType(msg)

        data_converted = restore_data_after_data_time_conversion(
            data=array_id_data_converted_values_all,
            data_backup=data_backup,
            converted_column_name=column_name
        )

    return data_converted


def process_array_ids(site, location, datalogger, data, time_zone, time_format_args_library,
                      output_dir, array_ids_info, file_ext):
    """Splits apart mixed array location files into subfiles based on each rows' array id.

    Parameters
    ----------
    site : str
        Site id.
    location : str
        Location id.
    datalogger : str
        Datalogger id.
    data : dict of DataSet
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

        logger_info.info("Processing array: {array_name}".format(array_name=array_name))
        array_id_data = data.get(array_name)
        logger_info.info("{num} new rows".format(num=len(array_id_data)))

        if not array_id_data:
            logger_info.info("No work to be done for array: {array_name}".format(array_name=array_name))
            continue

        column_names = array_id_info.get('column_names')
        logger_debug.debug("Column names : {column_names}".format(column_names=column_names))

        export_columns = array_id_info.get('export_columns')
        logger_debug.debug(
            "Export columns: {export_columns}".format(export_columns=export_columns))

        include_time_zone = array_id_info.get('include_time_zone', False)
        logger_debug.debug("Include time zone: {include_time_zone}".format(
            include_time_zone=include_time_zone))

        time_columns = array_id_info.get('time_columns')
        logger_debug.debug("Time columns: {time_columns}".format(time_columns=time_columns))

        time_parsed_column_name = array_id_info.get('time_parsed_column_name', 'Timestamp')
        logger_debug.debug("Time parsed column {time_parsed_column_name}".format(
            time_parsed_column_name=time_parsed_column_name))

        to_utc = array_id_info.get('to_utc', False)
        logger_debug.debug("To UTC {to_utc}".format(to_utc=to_utc))

        column_values_to_convert = array_id_info.get('convert_data_column_values')
        logger_debug.debug("Convert column_values: {column_values_to_convert}".format(
            column_values_to_convert=column_values_to_convert))

        array_id_file = array_name + file_ext
        logger_debug.debug("Array id file: {array_id_file}".format(
            array_id_file=array_id_file))

        array_id_file_path = os.path.join(
            os.path.abspath(output_dir), site, location, datalogger, array_id_file)
        logger_debug.debug("Array id file path: {array_id_file_path}".format(
            array_id_file_path=array_id_file_path))

        array_id_mismatches_file = array_name + ' Mismatches' + file_ext
        logger_debug.debug("Array id mismatched file: {array_id_mismatches_file}".format(
            array_id_mismatches_file=array_id_mismatches_file))

        array_id_mismatches_file_path = os.path.join(
            os.path.abspath(output_dir), site, location, datalogger, array_id_mismatches_file)
        logger_debug.debug(
            "Array id mismatched file path: {array_id_mismatches_file_path}".format(
                array_id_mismatches_file_path=array_id_mismatches_file_path))

        logger_info.info("Assigning column names")

        array_id_data_with_column_names, mismatches = cr.update_column_names(
            data=array_id_data,
            column_names=column_names,
            match_row_lengths=True,
            get_mismatched_row_lengths=True)

        logger_info.info("Number of matched row lengths: {matched}".format(
            matched=len(array_id_data_with_column_names)))
        logger_info.info("Number of mismatched row lengths: {mismatched}".format(
            mismatched=len(mismatches)))

        if column_values_to_convert:
            array_id_data_with_column_names = convert_data_column_values(
                data=array_id_data_with_column_names,
                values_to_convert=column_values_to_convert,
                time_zone=time_zone,
                time_format_args_library=time_format_args_library,
                to_utc=to_utc
            )

        array_id_data_time_converted = cr.parse_time(
            data=array_id_data_with_column_names,
            time_zone=time_zone,
            time_format_args_library=time_format_args_library,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc)

        data_to_export = make_export_data_set(
            data=array_id_data_time_converted, columns_to_export=export_columns)

        cr.export_to_csv(
            data=data_to_export,
            outfile_path=array_id_file_path,
            export_header=True,
            include_time_zone=include_time_zone
        )

        if mismatches:
            cr.export_to_csv(data=mismatches, outfile_path=array_id_mismatches_file_path)


def process_mixed_array(cfg, output_dir, site, location, datalogger, datalogger_info, track=False):
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
    datalogger : str
        Datalogger id.
    datalogger_info : dict
        Datalogger information including the datalogger's array ids lookup table, source file
        path and last read line number.
    track: If true, update configuration file with the last read line number.

    Returns
    -------
        Updated configuration file.

    """
    array_ids_info = datalogger_info.get('array_ids', {})
    logger_debug.debug(
        "Array ids info: {array_ids_info}".format(array_ids_info=array_ids_info))

    file_path = datalogger_info.get('file_path')
    logger_debug.debug("File path: {file_path}".format(file_path=file_path))

    line_num = datalogger_info.get('line_num', 0)
    logger_debug.debug("Line num: {line_num}".format(line_num=line_num))

    time_zone = datalogger_info.get('time_zone')
    logger_debug.debug("Time zone: {time_zone}".format(time_zone=time_zone))

    time_format_args_library = datalogger_info.get('time_format_args_library')
    logger_debug.debug("Time format args library: {time_format_args_library}".format(
        time_format_args_library=time_format_args_library))

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

    logger_info.info("Found {num} new rows".format(num=num_of_new_rows))
    if num_of_new_rows == 0:
        logger_info.info("No work to be done for location: {location}".format(location=location))
        return cfg

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension
    logger_debug.debug("File ext: {file_ext}".format(file_ext=file_ext))

    process_array_ids(
        site=site,
        location=location,
        datalogger=datalogger,
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        output_dir=output_dir,
        array_ids_info=array_ids_info,
        file_ext=file_ext
    )

    if track:
        if num_of_new_rows > 0:
            new_line_num = line_num + num_of_new_rows
            logger_info.info("Updated up to line number {num}".format(num=new_line_num))
            cfg['sites'][site]['locations'][location]['dataloggers'][datalogger]['line_num'] = new_line_num

    msg = "Done processing datalogger: {datalogger}"
    logger_info.info(msg.format(datalogger=datalogger))

    return cfg


def process_table_based(cfg, output_dir, site, location, datalogger, table, table_info, track=False):
    """
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
    datalogger : str
        Datalogger id.
    table : str
        Table-based file id.
    table_info : dict
        Table-based file information.
    track: If true, update configuration file with the last read line number.

    Returns
    -------
        Updated configuration file.

    """
    header_row = table_info.get('header_row')
    logger_debug.debug("Header row: {header_row}".format(header_row=header_row))

    column_names = table_info.get('column_names')
    logger_debug.debug("Column names: {column_names}".format(column_names=column_names))

    export_columns = table_info.get('export_columns')
    logger_debug.debug("Export columns: {export_columns}".format(export_columns=export_columns))

    name = table_info.get('name', table)
    logger_debug.debug("Name: {name}".format(name=name))

    convert_column_values = table_info.get('convert_data_column_values')
    logger_debug.debug("Convert column values: {convert_column_values}".format(
        convert_column_values=convert_column_values))

    file_path = table_info.get('file_path')
    logger_debug.debug("File path: {file_path}".format(file_path=file_path))

    line_num = table_info.get('line_num', 0)
    logger_debug.debug("Line num: {line_num}".format(line_num=line_num))

    time_columns = table_info.get('time_columns')
    logger_debug.debug("Time columns: {time_columns}".format(time_columns=time_columns))

    time_format_args_library = table_info.get('time_format_args_library')
    logger_debug.debug("Time format args library: {time_format_args_library}".format(
        time_format_args_library=time_format_args_library))

    time_parsed_column_name = table_info.get('time_parsed_column_name')
    logger_debug.debug("Time parsed column name: {time_parsed_column_name}".format(
        time_parsed_column_name=time_parsed_column_name))

    time_zone = table_info.get('time_zone')
    logger_debug.debug("Time zone: {time_zone}".format(time_zone=time_zone))

    to_utc = table_info.get('to_utc', False)
    logger_debug.debug("To UTC: {to_utc}".format(to_utc=to_utc))

    include_time_zone = table_info.get('include_time_zone', False)
    logger_debug.debug("Include time zone: {include_time_zone}".format(include_time_zone=include_time_zone))

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension
    logger_debug.debug("File ext: {file_ext}".format(file_ext=file_ext))

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
        raise NoHeadersException("Headers representation not found!")

    num_of_new_rows = 0
    num_of_new_rows += len(data)

    logger_info.info("Found {num} new rows".format(num=num_of_new_rows))
    if num_of_new_rows == 0:
        logger_info.info("No work to be done for table: {table}".format(table=name))
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
        os.path.abspath(output_dir), site, location, datalogger, file_name)

    cr.export_to_csv(
        data=data_to_export,
        outfile_path=outfile_path,
        export_header=True,
        include_time_zone=include_time_zone
    )

    if track:
        if num_of_new_rows > 0:
            new_line_num = line_num + num_of_new_rows
            logger_info.info("Updated up to line number {num}".format(num=new_line_num))
            cfg['sites'][site]['locations'][location]['dataloggers'][datalogger]['tables'][table]['line_num'] = new_line_num

    logger_info.info("Done processing table {table}".format(table=table))

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
        logger_info.info(msg)

    logger_debug.debug("Output directory: {dir}".format(dir=output_dir))
    logger_debug.debug("Getting configured sites.")

    sites = cfg['sites']

    configured_sites_msg = ', '.join("{site}".format(site=site) for site in sites)
    logger_debug.debug("Configured sites: {sites}.".format(sites=configured_sites_msg))

    if args.track:
        logger_info.info("Tracking is enabled.")
    else:
        logger_info.info("Tracking is disabled.")

    if args.site:
        # Process specific site
        logger_info.info("Processing site: {site}".format(site=args.site))
        site_info = sites[args.site]
        logger_debug.debug("Getting configured locations.")
        locations = site_info['locations']
        configured_locations_msg = ', '.join("{location}".format(
            location=location) for location in locations)
        logger_debug.debug("Configured locations: {locations}.".format(
            locations=configured_locations_msg))
        if args.location:
            # Process specific location
            logger_info.info("Processing location: {location}".format(location=args.location))
            location_info = locations[args.location]
            logger_debug.debug("Getting location configuration.")
            dataloggers = location_info['dataloggers']
            configured_dataloggers_msg = ', '.join("{datalogger}".format(
                datalogger=datalogger) for datalogger in dataloggers)
            logger_debug.debug("Configured dataloggers: {dataloggers}.".format(
                dataloggers=configured_dataloggers_msg))
            if args.datalogger:
                # Process specific datalogger
                logger_info.info(
                    "Processing datalogger: {datalogger}".format(datalogger=args.datalogger))
                datalogger_info = dataloggers[args.datalogger]
                logger_debug.debug("Getting datalogger memory structure.")
                memory_structure = datalogger_info['memory_structure']
                if memory_structure == 'mixed array':
                    cfg = process_mixed_array(
                        cfg, output_dir, args.site, args.location, args.datalogger,
                        datalogger_info, args.track)
                elif memory_structure == 'table based':
                    tables = datalogger_info['tables']
                    configured_tables_msg = ', '.join("{table}".format(
                        table=table) for table in tables)
                    logger_debug.debug("Configured tables: {tables}.".format(
                        tables=configured_tables_msg))
                    if args.table:
                        # Process specific table based file
                        table_info = tables[args.table]
                        cfg = process_table_based(
                            cfg, output_dir, args.site, args.location, args.datalogger,
                            args.table, table_info, args.track
                        )
                    else:
                        # Process all table based files
                        for table, table_info in tables.items():
                            cfg = process_table_based(
                                cfg, output_dir, args.site, args.location, args.datalogger,
                                table, table_info, args.track
                            )
                else:
                    raise TypeError("Unsupported datalogger memory structure type!")
            else:
                # Process all dataloggers
                for datalogger, datalogger_info in dataloggers.items():
                    logger_info.info(
                        "Processing datalogger: {datalogger}".format(
                            datalogger=datalogger))
                    memory_structure = datalogger_info['memory_structure']
                    if memory_structure == 'mixed array':
                        cfg = process_mixed_array(
                            cfg, output_dir, args.site, args.location, datalogger,
                            datalogger_info, args.track)
                    elif memory_structure == 'table based':
                        tables = datalogger_info['tables']
                        configured_tables_msg = ', '.join("{table}".format(
                            table=table) for table in tables)
                        logger_debug.debug("Configured tables: {tables}.".format(
                            tables=configured_tables_msg))
                        # Process all table based files
                        for table, table_info in tables.items():
                            cfg = process_table_based(
                                cfg, output_dir, args.site, args.location, datalogger,
                                table, table_info, args.track
                            )
                    else:
                        raise TypeError("Unsupported datalogger memory structure type!")

        else:
            # Process all locations
            for location, location_info in locations.items():
                logger_info.info(
                    "Processing location: {location}".format(location=location))
                logger_debug.debug("Getting location configuration.")
                dataloggers = location_info['dataloggers']
                configured_dataloggers_msg = ', '.join("{datalogger}".format(
                    datalogger=datalogger) for datalogger in dataloggers)
                logger_debug.debug("Configured dataloggers: {dataloggers}.".format(
                    dataloggers=configured_dataloggers_msg))
                # Process all dataloggers
                for datalogger, datalogger_info in dataloggers.items():
                    logger_info.info(
                        "Processing datalogger: {datalogger}".format(
                            datalogger=datalogger))
                    memory_structure = datalogger_info['memory_structure']
                    if memory_structure == 'mixed array':
                        cfg = process_mixed_array(
                            cfg, output_dir, args.site, location, datalogger,
                            datalogger_info, args.track)
                    elif memory_structure == 'table based':
                        tables = datalogger_info['tables']
                        configured_tables_msg = ', '.join("{table}".format(
                            table=table) for table in tables)
                        logger_debug.debug("Configured tables: {tables}.".format(
                            tables=configured_tables_msg))
                        # Process all table based files
                        for table, table_info in tables.items():
                            cfg = process_table_based(
                                cfg, output_dir, args.site, location, datalogger, table,
                                table_info, args.track
                            )
                    else:
                        raise TypeError(
                            "Unsupported datalogger memory structure type!")

        logger_info.info("Done processing site: {site}".format(site=args.site))
    else:
        # Process all sites
        for site, site_info in sites.items():
            logger_info.info("Processing site: {site}".format(site=site))
            locations = site_info['locations']
            configured_locations_msg = ', '.join("{location}".format(
                location=location) for location in locations)
            logger_debug.debug("Configured locations: {locations}.".format(
                locations=configured_locations_msg))
            # Process all locations
            for location, location_info in locations.items():
                logger_info.info(
                    "Processing location: {location}".format(location=location))
                logger_debug.debug("Getting location configuration.")
                dataloggers = location_info['dataloggers']
                configured_dataloggers_msg = ', '.join("{datalogger}".format(
                    datalogger=datalogger) for datalogger in dataloggers)
                logger_debug.debug("Configured dataloggers: {dataloggers}.".format(
                    dataloggers=configured_dataloggers_msg))
                # Process all dataloggers
                for datalogger, datalogger_info in dataloggers.items():
                    logger_info.info(
                        "Processing datalogger: {datalogger}".format(
                            datalogger=datalogger))
                    memory_structure = datalogger_info['memory_structure']
                    if memory_structure == 'mixed array':
                        cfg = process_mixed_array(
                            cfg, output_dir, site, location, datalogger,
                            datalogger_info, args.track)
                    elif memory_structure == 'table based':
                        tables = datalogger_info['tables']
                        configured_tables_msg = ', '.join("{table}".format(
                            table=table) for table in tables)
                        logger_debug.debug("Configured tables: {tables}.".format(
                            tables=configured_tables_msg))
                        # Process all table based files
                        for table, table_info in tables.items():
                            cfg = process_table_based(
                                cfg, output_dir, site, location, datalogger, table,
                                table_info, args.track
                            )
                    else:
                        raise TypeError(
                            "Unsupported datalogger memory structure type!")

            logger_info.info("Done processing site: {site}".format(site=args.site))

    if args.track:
        logger_info.info("Updating config file.")
        utils.save_config(APP_CONFIG_PATH, cfg)


def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='LoggerFilesFormatter',
        description='Program for formatting and exporting Campbell Scientific datalogger files.'
    )
    parser.add_argument('-s', '--site', action='store', dest='site',
                        help='Site to process.')
    parser.add_argument('-l', '--location', action='store', dest='location',
                        help='Location to process.')
    parser.add_argument('-d', '--datalogger', action='store', dest='datalogger',
                        help='Datalogger to process.')
    parser.add_argument('-f', '--table', action='store', dest='table',
                        help='Table based file to process.')
    parser.add_argument(
        '-t', '--track',
        help='Track file line number.',
        dest='track',
        action='store_true',
        default=False
    )

    args = parser.parse_args()

    logger_debug.debug("Arguments passed by user")
    args_msg = ', '.join("{arg}: {value}".format(
        arg=arg, value=value) for (arg, value) in vars(args).items())
    logger_debug.debug(args_msg)

    if args.location and not args.site:
        parser.error("--site is required.")
    if args.table:
        if not args.location or not args.site:
            parser.error("--site and --location is required.")

    app_cfg = utils.load_config(APP_CONFIG_PATH)

    system_is_active = app_cfg['settings']['active']
    if not system_is_active:
        logger_info.info("System is not active.")
        return

    logger_info.info("System is active")
    logger_info.info("Initializing")

    start = time.time()
    process_sites(app_cfg, args)
    stop = time.time()
    elapsed_time = (stop - start)

    logger_info.info("Finished job in {elapsed_time} seconds".format(elapsed_time=elapsed_time))

if __name__ == '__main__':
    main()
    logger_info.info("Exiting.")
