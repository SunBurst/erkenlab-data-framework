#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Script for collecting and exporting Campbell CR10X mixed-array files. """

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse

from campbellsciparser import device

from loggerfileupdater import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr10xsplitter.yaml')


def process_location(cfg, output_dir, site, location, location_info, track=False):
    """Splits apart the mixed-array location file into subfiles based on each rows' array id, i.e. first element.

    Args:
        cfg (dict): Program's configuration file.
        output_dir (string): Output directory.
        site (string): Site id.
        location (string): Location id.
        location_info (dict): Location information including the location's array ids lookup table, source file
            path and last read line number.
        track: If true, update configuration file with the last read line number.

    Returns:
        Updated configuration file.

    """
    array_ids = location_info.get('array ids')
    file_path = location_info.get('file_path')
    file_ext = os.path.splitext(os.path.abspath(file_path))[1]  # Get file extension, e.g. '.dat', '.csv' etc.
    line_num = location_info.get('line_num', 0)
    table = {array_id: array_id_info.get('name', array_id) for array_id, array_id_info in array_ids.items()}
    data = device.CR10X.get_array_ids_data(file_path=file_path, line_num=line_num, fix_floats=True, array_ids_table=table)

    num_of_new_rows = 0

    for array_id, array_id_data in data.items():
        num_of_new_rows += len(array_id_data)

    print("Found {0} new rows".format(num_of_new_rows))

    export_info = {}

    for array_id, array_id_info in array_ids.items():
        array_name = array_id_info.get('name', array_id)
        array_header = array_id_info.get('header')
        subfile_name = array_name + file_ext
        subfile_path = os.path.join(
            os.path.abspath(output_dir), site, location, subfile_name)	# Construct absolute file path to subfile.
        export_info[array_name] = {'file_path': subfile_path, 'header': array_header}
    device.CR10X.export_array_ids_to_csv(data=data, array_ids_info=export_info)

    if track:
        if num_of_new_rows > 0:
            cfg['sites'][site]['locations'][location]['line_num'] = line_num + num_of_new_rows

    print("Done processing site {0}, location {1}".format(site, location))

    return cfg


def process_files(args):
    """Unpacks data from the configuration file, calls the core function and updates line number information if tracking
        is enabled.

    Args:
        args (Namespace): Arguments passed by the user. Includes site, location and tracking information.

    """
    cfg = utils.load_config(CONFIG_PATH)

    try:
        output_dir = cfg['settings']['output_dir']
    except KeyError:
        output_dir = os.path.expanduser("~")
        print("No output directory set, files will be output to the user default directory at {0}".format(output_dir))

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
    parser = argparse.ArgumentParser(prog='CR10XSplitter',
                                     description='Program for splitting Campbell CR10X mixed-array data logger files.')
    parser.add_argument('-s', '--site', action='store', dest='site',
                        help='Site to split.')
    parser.add_argument('-l', '--location', action='store', dest='location',
                        help='Location to split.')
    parser.add_argument('-t', '--track', help='Track file line number.', dest='track', action='store_true',
                        default=False)

    args = parser.parse_args()

    if args.location and not args.site:
        parser.error("--site and --location are required.")

    process_files(args)

if __name__ == '__main__':
    main()
