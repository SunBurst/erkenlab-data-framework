#!/usr/bin/env
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse

from collections import defaultdict

from campbellsciparser import device

from loggerfileupdater import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cr10x.yaml')


def to_subfiles(file, output_dir, site, location, version, array_ids, line_num=0):
    """Split csv file into subfiles based on its first element.

    Args:
        file (string): Source file's absolute path.
        output_dir (string): Output directory path.
        site (string): Site name, used for naming subfiles.
        location (string): Location name, used for naming subfiles.
        version (string): Version name, used for naming subfiles.
        array_ids (dict): Array ID:s lookup table for translating array ID:s to plain text and for
            exporting file headers.
        line_num (int): File line number, used for tracking.

    Returns:
        The number of lines processed.

    """

    def _identify_array_id(row):
        """Identifies the array id of the given row.

        Args:
            row (list): Reading to identify.
        Returns:
            The array id of the given row.

        """
        return row[0]


    def _fix_floating_points(_line):
        """Replaces . with .0 and -. with -.0.

        Args:
            _line (string): The line to be fixed.

        Returns:
            Fixed line.

        """
        _replacements = {',.' : ',0.', ',-.' : ',-0.'}  # Library of patterns to look for and its replacement.
        for source, replace in _replacements.items():   # Iterate through replacement library to look for patterns in current line.
            _line = _line.replace(source, replace)
        return _line

    def _process_line(_line):
        """Fixes floating points, breaks the string into a list of string elements and identifies its array id.

        Args:
            _line (string): The line to be fixed.

        Returns:
            Array id and fixed line as a list.

        """
        _line_fl_fixed = _fix_floating_points(_line)
        _line_fl_fixed_as_list = _line_fl_fixed.split(',')    # Split string into a list of string elements on comma, e.g. "a,b,c" -> ["a","b","c"].
        _array_id = _identify_array_id(_line_fl_fixed_as_list)
        return _array_id, _line_fl_fixed_as_list


    file_ext = os.path.splitext(os.path.abspath(file))[1]	# Get file extension, e.g. '.dat', '.csv' etc.
    #array_ids_dict = defaultdict(list)    # Setup array id dictionary to hold list of lists in order to preserve \
    array_ids_dict = defaultdict(lambda: defaultdict(list))                                # their order, e.g. {array_id : [[row_1], [row_2], [row_n]]}
    f_list = utils.open_file(file)
    num_of_new_rows = 0

    for line_number, line in enumerate(f_list):
        if (line_num <= line_number):   # If current line is new, process it.
            array_id, line_fixed = _process_line(line)
            num_of_columns = len(line_fixed)
            array_ids_dict[array_id][num_of_columns].append(line_fixed)   # Append fixed line to its corresponding array id.
            num_of_new_rows += 1

    for array_id, col_size_dict in array_ids_dict.items():
        array_name = array_id
        parameters = None
        if array_ids:
            array_id_data = array_ids.get(array_id)
            array_name = array_id_data.get('name', array_id)
            parameters = array_id_data.get('parameters')
        for col_size, rows_list in col_size_dict.items():
            header = None
            param_order_name = ' {0}'.format(col_size) + ' columns'
            if parameters:
                if col_size == len(parameters.get('default')):
                    header = parameters.get('default')
                    param_order_name = ""
                else:
                    for param_order_id, param_order in parameters.items():
                        if col_size == len(param_order):
                            header = param_order
                            param_order_name = param_order_id
            file_name = array_name + param_order_name + file_ext
            fixed_file = os.path.join(
                os.path.abspath(output_dir), site, location, version, file_name)	# Construct absolute file path to subfile.
            os.makedirs(os.path.dirname(fixed_file), exist_ok=True)    # Create file if it doesn't already exists.

            if os.path.exists(fixed_file):
                f_list = utils.open_file(fixed_file)
                if len(f_list) > 0:
                    header = None

            f_out = open(fixed_file, 'a')   # Open file in append-only mode.
            if header:
                f_out.write(",".join(header) + "\n")
            for i, row in enumerate(rows_list):
                f_out.write((",".join(row)) + "\n")
            f_out.close()

    return num_of_new_rows


def process_files(args):
    """Unpacks data from the configuration file, calls the core function and updates information if tracking
        is enabled.

    Args:
        args (Namespace): Arguments passed by the user. Includes site and location information.

    """
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    sites = cfg['sites']

    if args.site:
        site_data = sites.get(args.site)
        locations = site_data.get('locations')
        if args.location:
            location_data = locations.get(args.location)
            logger_prog_versions = location_data.get('versions')
            if args.version:
                version_data = logger_prog_versions.get(args.version)
                file_path = version_data.get('file_path')
                array_ids = version_data.get('array_ids')
                num_of_new_rows = 0
                print("Processing site {0}, location {1}, version {2}...".format(args.site, args.location, args.version))
                if args.track:
                    line_num = version_data.get('line_num')
                    num_of_new_rows = to_subfiles(file_path, output_dir, args.site, args.location, args.version, array_ids, line_num)
                    if num_of_new_rows > 0:
                        cfg['sites'][args.site]['locations'][args.location]['versions'][args.version]['line_num'] = line_num + num_of_new_rows
                else:
                    num_of_new_rows = to_subfiles(file_path, output_dir, args.site, args.location, args.version, array_ids)

                print("Processed site {0}, location {1}".format(args.site, args.location))
                print("Number of new rows: {0}".format(num_of_new_rows))
            else:
                for v, version_data in logger_prog_versions.items():
                    file_path = version_data.get('file_path')
                    array_ids = version_data.get('array_ids')
                    num_of_new_rows = 0
                    print("Processing site {0}, location {1}, version {2}...".format(args.site, args.location, v))
                    if args.track:
                        line_num = version_data.get('line_num')
                        num_of_new_rows = to_subfiles(file_path, output_dir, args.site, args.location, v, array_ids, line_num)
                        if num_of_new_rows > 0:
                            cfg['sites'][args.site]['locations'][args.location]['versions'][args.version]['line_num'] = line_num + num_of_new_rows
                    else:
                        num_of_new_rows = to_subfiles(file_path, output_dir, args.site, args.location, v, array_ids)

                    print("Processed site {0}, location {1}".format(args.site, args.location))
                    print("Number of new rows: {0}".format(num_of_new_rows))
        else:
            for l, location_data in locations.items():
                logger_prog_versions = location_data.get('versions')
                for v, version_data in logger_prog_versions.items():
                    file_path = version_data.get('file_path')
                    array_ids = version_data.get('array_ids')
                    num_of_new_rows = 0
                    print("Processing site {0}, location {1}, version {2}...".format(args.site, args.location, v))
                    if args.track:
                        line_num = version_data.get('line_num')
                        num_of_new_rows = to_subfiles(file_path, output_dir, args.site, l, v, array_ids, line_num)
                        if num_of_new_rows > 0:
                            cfg['sites'][args.site]['locations'][args.location]['versions'][args.version]['line_num'] = line_num + num_of_new_rows
                    else:
                        num_of_new_rows = to_subfiles(file_path, output_dir, args.site, l, v, array_ids)

                    print("Processed site {0}, location {1}".format(args.site, args.location))
                    print("Number of new rows: {0}".format(num_of_new_rows))
    else:
        for s, site_data in sites.items():
            locations = site_data.get('locations')
            for l, location_data in locations.items():
                logger_prog_versions = location_data.get('versions')
                for v, version_data in logger_prog_versions.items():
                    file_path = version_data.get('file_path')
                    array_ids = version_data.get('array_ids')
                    num_of_new_rows = 0
                    print("Processing site {0}, location {1}, version {2}...".format(args.site, args.location, v))
                    if args.track:
                        line_num = version_data.get('line_num')
                        num_of_new_rows = to_subfiles(file_path, output_dir, s, l, v, array_ids, line_num)
                        if num_of_new_rows > 0:
                            cfg['sites'][args.site]['locations'][args.location]['versions'][args.version]['line_num'] = line_num + num_of_new_rows
                    else:
                        num_of_new_rows = to_subfiles(file_path, output_dir, s, l, v, array_ids)

                    print("Processed site {0}, location {1}".format(args.site, args.location))
                    print("Number of new rows: {0}".format(num_of_new_rows))

    print("Updating config file.")
    utils.save_config(CONFIG_PATH, cfg)


def main():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='FileSplitter',
                                     description='Program for splitting Campbell CR10X mixed-array datalogger files.')
    parser.add_argument('-s', '--site', action='store', dest='site',
                        help='Specific site to manage.')
    parser.add_argument('-l', '--location', action='store', dest='location',
                        help='Specific location to manage.')
    parser.add_argument('-t', '--track', help='Track file line number.', dest='track', action='store_true',
                        default=False)

    args = parser.parse_args()

    if args.location and not args.site:
        parser.error("--site and --location are required.")

    process_files(args)

if __name__=='__main__':
    main()
