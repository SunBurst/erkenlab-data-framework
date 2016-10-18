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
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr1000formatter.yaml')


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
    header_row = int(file_info.get('header_row', 0))
    export_columns = file_info.get('export_columns')
    name = file_info.get('name', file)
    file_path = file_info.get('file_path')
    line_num = file_info.get('line_num', 0)
    time_columns = file_info.get('time_columns')
    time_parsed_column_name = file_info.get('time_parsed_column_name')
    time_zone = file_info.get('time_zone')
    to_utc = file_info.get('to_utc', False)
    include_time_zone = file_info.get('include_time_zone', False)

    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension

    cr1000parser = devices.CR1000Parser(time_zone)
    data = cr1000parser.read_data(
        infile_path=file_path,
        header_row=header_row,
        line_num=line_num,
        convert_time=True,
        time_parsed_column=time_parsed_column_name,
        time_columns=time_columns,
        to_utc=to_utc
    )

    num_of_new_rows = 0
    num_of_new_rows += len(data)

    print("Found {0} new rows".format(num_of_new_rows))

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


def process_files(args):
    """Unpacks data from the configuration file, calls the core function and updates
        line number information if tracking is enabled.

    Args
    ----
    args (Namespace): Arguments passed by the user. Includes site, location, file and
        tracking information.

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
            files = location_info.get('files')
            if args.file:
                file_info = files.get(args.file)
                cfg = process_file(
                    cfg,
                    output_dir,
                    args.site,
                    args.location,
                    args.file,
                    file_info,
                    track=False)

        else:
            for location, location_info in locations.items():
                files = location_info.get('files')
                for file, file_info in files.items():
                    cfg = process_file(
                        cfg,
                        output_dir,
                        args.site,
                        location,
                        file,
                        file_info,
                        track=False)
    else:
        for site, site_info in sites.items():
            locations = site_info.get('locations')
            for location, location_info in locations.items():
                files = location_info.get('files')
                for file, file_info in files.items():
                    cfg = process_file(
                        cfg,
                        output_dir,
                        site,
                        location,
                        file,
                        file_info,
                        track=False)

    if args.track:
        print("Updating config file.")
        utils.save_config(CONFIG_PATH, cfg)


def main():
    """Parses and validates arguments from the command line. """
    parser = argparse.ArgumentParser(
        prog='CR1000 Exporter',
        description='Program for exporting Campbell CR1000 datalogger files.'
    )
    parser.add_argument('-s', '--site', action='store', dest='site',
                        help='Site to split.')
    parser.add_argument('-l', '--location', action='store', dest='location',
                        help='Location to split.')
    parser.add_argument('-f', '--file', action='store', dest='file',
                        help='File to split.')
    parser.add_argument(
        '-t', '--track',
        help='Track file line number.',
        dest='track',
        action='store_true',
        default=False
    )

    args = parser.parse_args()

    if args.file:
        if not args.location and not args.site:
            parser.error("--site and --location are required.")
    if args.location and not args.site:
        parser.error("--site is required.")

    process_files(args)

if __name__ == '__main__':
    main()
