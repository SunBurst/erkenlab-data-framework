#!/usr/bin/env
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import defaultdict
from datetime import datetime

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/filesplitter.yaml')

def identify_array_id(row):
    """Identifies the array id of the given row.

    Args:
        row (list): Reading to identify.
    Returns:
        The array id of the given row.

    """
    return row[0]


def to_subfiles(file, output_dir, site=None, location=None, array_ids=None, line_num=0):
    """Split csv file into subfiles based on its first element.

    Args:
        site (string): Site name, used for naming subfiles.
        location (string): Location name, used for naming subfiles.
        array_ids (dict): Array ID:s lookup table for translating codes to plain text.
        file (string): Source file's absolute path.
        line_num (int): File line number, used for tracking.

    Returns:
        The number of lines processed.

    """

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
        _array_id = identify_array_id(_line_fl_fixed_as_list)
        return _array_id, _line_fl_fixed_as_list

    if not site:
        site = "Unknown site " + datetime.now().strftime("%Y-%m-%d %H %M %S")
    if not location:
        location = "Unknown location " + datetime.now().strftime("%Y-%m-%d %H %M %S")

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
        if array_ids:
            array_id = array_ids.get(array_id, array_id)    # Try to obtain plain text representation.
        for col_size, rows_list in col_size_dict.items():
            fixed_file = os.path.join(
                os.path.abspath(output_dir), site, location,
                array_id + ' {0}'.format(col_size) + ' columns' + file_ext)	# Construct absolute file path to subfile.
            os.makedirs(os.path.dirname(fixed_file), exist_ok=True)    # Create file if it doesn't already exists.
            f_out = open(fixed_file, 'a')   # Open file in append-only mode.
            for i, row in enumerate(rows_list):
                f_out.write((",".join(row)) + "\n")
            f_out.close()

    return num_of_new_rows


def run_file(file_input, file_output):
    """Process single file.

    Args:
        file_input (string): Absolute path to source file.
        file_output (string): Path to output directory.

    """
    to_subfiles(file_input, file_output)


def update_location(cfg, output_dir, site, location, location_data):
    """
        Fetches location information from configuration file, calls file splitting sub-script and updates file line
        number.

    Args:
        cfg (dict): Configuration file stored in memory.
        output_dir (string): Working directory.
        site (string): Site name.
        location (string): Location name.
        location_data (dict): Location information.

    """
    line_num = location_data.get('line_num')
    array_ids = location_data.get('array_ids')
    file_path = location_data.get('file_path')
    num_of_new_rows = to_subfiles(file_path, output_dir, site, location, array_ids, line_num)
    cfg['sites'][site][location]['line_num'] = line_num + num_of_new_rows


def run_system(**kwargs):
    """Run automatic system on one or more targets.

    Args:
        **kwargs: If not given, traverse all sites. If site is given, traverse that particular site. If site and location
            is given, run that particular location.

    """
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    if not kwargs:
        sites = cfg['sites']
        for site, site_data in sites.items():
            for location, location_data in site_data.items():
                if location_data.get('automatic_updating'):
                    update_location(cfg, output_dir, site, location, location_data)

    if 'site' in kwargs:
        site = kwargs.get('site')
        site_data = cfg['sites'][site]
        if not 'location' in kwargs:
            for location, location_data in site_data.items():
                if location_data.get('automatic_updating'):
                    update_location(cfg, output_dir, site, location, location_data)
        else:
            location = kwargs.get('location')
            location_data = cfg['sites'][site][location]
            if location_data.get('automatic_updating'):
                update_location(cfg, output_dir, site, location, location_data)

    utils.save_config(CONFIG_PATH, cfg)


def edit_cfg_file(parameters, replacements, old_values=None):
    """Edit specific parameters in the configuration file 'filesplitter.yaml'.

    Args:
        parameters (dict): Parameters needed to edit.
        replacements (dict): Replacement values.
        old_values (dict): Backup values needed for renaming sites and/or locations.

    """
    cfg = utils.load_config(CONFIG_PATH)
    if 'auto_updating' in replacements:
        cfg['sites'][parameters.get('site')][parameters.get('location')]['automatic_updating'] = replacements.get('auto_updating')
    if 'file_path' in replacements:
        cfg['sites'][parameters.get('site')][parameters.get('location')]['file_path'] = replacements.get('file_path')
    if 'line_num' in replacements:
        cfg['sites'][parameters.get('site')][parameters.get('location')]['line_num'] = replacements.get('line_num')
    if 'location' in replacements:
        cfg['sites'][parameters.get('site')][replacements.get('location')] = cfg['sites'][parameters.get('site')][old_values.get('location')]
        cfg['sites'][parameters.get('site')].pop(old_values.get('location'))
    if 'site' in replacements:
        cfg['sites'][replacements.get('site')] = cfg['sites'][old_values.get('site')]
        cfg['sites'].pop(old_values.get('site'))

    utils.save_config(CONFIG_PATH, cfg)


def process_args(args):
    """Process given input arguments.

    Args:
        args (Namespace): Validated arguments.

    """
    if args:
        if args.top_parser_name == 'system':
            if args.mode_parser_name == 'settings':
                if args.dir:
                    utils.change_dir(args.dir, CONFIG_PATH)
                if args.clean:
                    utils.clean_dir(args.clean, CONFIG_PATH)
            elif args.mode_parser_name == 'run':
                if args.site:
                    info = {'site' : args.site}
                    if args.location:
                        info['location'] = args.location
                    run_system(**info)
                else:
                    run_system()
            elif args.mode_parser_name == 'edit':
                parameters = vars(args)
                old_values = {}
                replacements = dict(zip(parameters.get('parameter'), parameters.get('replacement')))
                for key, val in replacements.items():
                    if key in parameters:
                        old_values[key] = parameters.get(key)

                edit_cfg_file(parameters, replacements, old_values)

            elif args.mode_parser_name == 'updating':
                parameters = vars(args)
                if args.auto_updating:
                    edit_cfg_file(parameters, {'auto_updating' : True})
                else:
                    edit_cfg_file(parameters, {'auto_updating' : False})

        elif args.top_parser_name == 'file':
            run_file(args.input, args.output)