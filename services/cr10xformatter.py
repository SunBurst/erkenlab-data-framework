#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Script for parsing and exporting Campbell CR10X mixed-array files. """

import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import OrderedDict

from campbellsciparser import devices

from services import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr10xformatter.yaml')


def process_location(cfg, output_dir, site, location, location_info, track=False):
    """Splits apart mixed-array location files into subfiles based on each rows' array id.

    Args
    ----
    cfg (dict): Program's configuration file.
    output_dir (string): Output directory.
    site (string): Site id.
    location (string): Location id.
    location_info (dict): Location information including the location's array ids
        lookup table, source file
        path and last read line number.
    track: If true, update configuration file with the last read line number.

    Returns
    -------
        Updated configuration file.

    """
    array_ids = location_info.get('array_ids')
    file_path = location_info.get('file_path')
    line_num = location_info.get('line_num', 0)
    time_zone = location_info.get('time_zone')
    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension
    array_id_names = {array_id: array_id_info.get('name', array_id) for array_id, array_id_info in array_ids.items()}

    cr10xparser = devices.CR10XParser(time_zone)
    data = devices.CR10XParser.read_array_ids_data(
        infile_path=file_path, line_num=line_num, fix_floats=True, array_id_names=array_id_names)

    num_of_new_rows = 0

    for array_id, array_id_data in data.items():
        num_of_new_rows += len(array_id_data)

    print("Found {0} new rows".format(num_of_new_rows))

    for array_id, array_id_info in array_ids.items():
        array_name = array_id_info.get('name', array_id)
        array_headers = array_id_info.get('headers')
        export_columns = array_id_info.get('export_columns')
        include_time_zone = array_id_info.get('include_time_zone', False)
        time_columns = array_id_info.get('time_columns')
        time_parsed_column_name = array_id_info.get('time_parsed_column_name', 'Timestamp')
        to_utc = array_id_info.get('to_utc', False)

        array_id_file = array_name + file_ext
        array_id_file_path = os.path.join(os.path.abspath(output_dir), site, location, array_id_file)
        array_id_mismatches_file = array_name + ' Mismatches' + file_ext
        array_id_mismaches_file_path = os.path.join(
            os.path.abspath(output_dir), site, location, array_id_mismatches_file)

        array_id_data = data.get(array_name)
        array_id_data_with_column_names, mismatches = cr10xparser.update_column_names(
            data=array_id_data,
            headers=array_headers,
            match_row_lengths=True,
            output_mismatched_rows=True)

        array_id_data_time_converted = cr10xparser.convert_time(
            data=array_id_data_with_column_names,
            time_parsed_column=time_parsed_column_name,
            time_columns=time_columns,
            to_utc=to_utc)

        data_to_export = []
        for row in array_id_data_time_converted:
            data_to_export.append(OrderedDict([(name, value) for name, value in row.items() if name in export_columns]))

        cr10xparser.export_to_csv(
            data=data_to_export,
            outfile_path=array_id_file_path,
            export_headers=True,
            include_time_zone=include_time_zone
        )

        if mismatches:
            cr10xparser.export_to_csv(data=mismatches, outfile_path=array_id_mismaches_file_path)

    if track:
        if num_of_new_rows > 0:
            cfg['sites'][site]['locations'][location]['line_num'] = line_num + num_of_new_rows

    print("Done processing site {0}, location {1}".format(site, location))

    return cfg


def process_files(args):
    """Unpacks data from the configuration file, calls the core function and updates line number information if tracking
        is enabled.

    Args
    ----
    args (Namespace): Arguments passed by the user. Includes site, location and tracking information.

    """
    cfg = utils.load_config(CONFIG_PATH)

    system_is_active = cfg['settings']['active']
    if not system_is_active:
        print("System not active.")
        return

    try:
        output_dir = cfg['settings']['data_output_dir']
    except KeyError:
        output_dir = os.path.expanduser("~")
        msg = "No output directory set! "
        msg += "Files will be output to the user's default directory at {output_dir}"
        msg = msg.format(output_dir=output_dir)
        print(msg)

    sites = cfg['sites']

    if args.site:
        site_info = sites.get(args.site)
        locations = site_info.get('locations')
        if args.location:
            location_info = locations.get(args.location)
            cfg = process_location(cfg, output_dir, args.site, args.location, location_info, args.track)
        else:
            for location, location_info in locations.items():
                cfg = process_location(cfg, output_dir, args.site, location, location_info, args.track)
    else:
        for site, site_info in sites.items():
            locations = site_info.get('locations')
            for location, location_info in locations.items():
                cfg = process_location(cfg, output_dir, site, location, location_info, args.track)

    if args.track:
        print("Updating config file.")
        utils.save_config(CONFIG_PATH, cfg)


def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='CR10X Exporter',
        description='Program for splitting Campbell CR10X mixed-array data logger files.'
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

    if args.location and not args.site:
        parser.error("--site and --location are required.")

    process_files(args)

if __name__ == '__main__':
    main()
