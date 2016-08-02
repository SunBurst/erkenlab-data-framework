import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

import pytz

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/timeconverter.yaml')

class RawDataTimeManager(object):
    """Class for converting raw data time formats to timestamp. """

    def __init__(self, time_zone):
        """Class initializer.
        Args: time_zone (string): Raw data pytz time zone.
        """

        self.tz = pytz.timezone(time_zone)

    def parse_custom_format(self, *args):
        pass

    def parse_time(self, *args):
        """Converts the given raw data time format to a datetime object (local time zone -> UTC).
        Args:
            args:
        """
        (time_fmt, time_str) = self.parse_custom_format(*args)
        t = time.strptime(time_str, time_fmt)
        dt = datetime.fromtimestamp(time.mktime(t))
        loc_dt = self.tz.localize(dt)
        utc_dt = loc_dt.astimezone(pytz.utc)

        return utc_dt

class CampbellLegacy(RawDataTimeManager):

    def __init__(self, time_zone='UTC'):
        RawDataTimeManager.__init__(self, time_zone)
        self.time_fmt_lib = ['%Y', '%j', 'HourMinute']

    def parse_custom_format(self, *args):
        """Parses the custom format 'Campbell Legacy'.
        Args:
            time_args_dict: Map of time format and its raw value.
        Returns:
            Adjusted time format code and its parsed value.
        """

        time_args = list(args)
        temp_lib = []

        for i, time_arg in enumerate(time_args):
            temp_lib.append(self.time_fmt_lib[i])
            if(i == 2):     # hourminute reached
                (time_fmt_parsed, time_str_parsed) = self.parse_hourminute(time_arg)
                temp_lib[2] = time_fmt_parsed
                time_args[2] = time_str_parsed

        time_fmt_str = ','.join(temp_lib)
        time_vals_str = ','.join(time_args)

        return (time_fmt_str, time_vals_str)

    def parse_hourminute(self, hm):
        """Method for parsing the custom format 'hourminute'.

        Args:
            hm (string): Input hourminute string to be parsed.
        Returns:
            The time parsed in the format HH:MM.
        """

        hm_int = int(hm)
        hour = int(hm_int/100)
        temp_min_hour = hm
        parsed_fmt = "%H:%M"
        parsed_time = ""

        if (len(temp_min_hour) == 1):            #: 0
            hour = 0
            minute = temp_min_hour
            parsed_time = "00:0" + minute
        elif (len(temp_min_hour) == 2):           #: 10 - 50
            hour = 0
            minute = temp_min_hour[-2:]
            parsed_time = "00:" + minute
        elif (len(temp_min_hour) == 3):          #: 100 - 950
            hour = temp_min_hour[:1]
            minute = temp_min_hour[-2:]
            parsed_time = "0" + hour + ":" + minute
        elif (len(temp_min_hour) == 4):          #: 1000 - 2350
            hour = temp_min_hour[:2]
            minute = temp_min_hour[-2:]
            parsed_time = hour + ":" + minute

        return (parsed_fmt, parsed_time)

class CampbellModern(RawDataTimeManager):

    def __init__(self, time_zone='UTC'):
        RawDataTimeManager.__init__(self, time_zone)

    def parse_custom_format(self, *args):

        try:
            time_str = args[0]
            time_str_parsed = time_str.strip('""')
            return ('%Y-%m-%d %H:%M:%S', time_str_parsed)
        except IndexError:
            pass


def file_time_to_timestamp(infile, output_dir, logger_time_format, time_args, raw_data_tz, target_tz = 'UTC',
                           site=None, location=None, file_name=None, line_num=0):

    def _process_line(_line, _raw_tm, _target_tz, _time_args_positions):
        """
        Args:
            _line (string): The line to be fixed.

        Returns:
            Array id and fixed line as a list.

        """
        _line_as_list = _line.split(',')    # Split string into a list of string elements on comma, e.g. "a,b,c" -> ["a","b","c"].

        print(_time_args_positions)
        print(_line)
        _time_args = [_line[i] for i in _time_args_positions]
        _dt_converted = _raw_tm.parse_time(*_time_args)
        _time_args_positions = sorted(_time_args_positions, reverse=True)

        for i, j in enumerate(_time_args_positions):
            if not (i == len(_time_args_positions) - 1):
                del _line_as_list[j]
            else:
                _line_as_list[j] = _dt_converted

        return _line_as_list

    file_ext = os.path.splitext(os.path.abspath(infile))[1]	# Get file extension, e.g. '.dat', '.csv' etc.
    f_list = utils.open_file(infile)

    if logger_time_format == 'campbell legacy':
        raw_tm = CampbellLegacy(raw_data_tz)
    elif logger_time_format == 'campbell modern':
        raw_tm = CampbellModern(raw_data_tz)

    num_of_new_rows = 0
    for line_number, line in enumerate(f_list):
        if (line_num <= line_number):   # If current line is new, process it.
            line_processed = _process_line(line, raw_tm, target_tz, time_args)
            print(line_processed)
            num_of_new_rows += 1
    print("{0} rows processed!".format(num_of_new_rows))

def update_location_file(cfg, output_dir, site, location, file, file_data):
    """
        Fetches file information from configuration file, calls file splitting sub-script and updates file line
        number.

    Args:
        cfg (dict): Configuration file stored in memory.
        output_dir (string): Working directory.
        site (string): Site name.
        location (string): Location name.
        location_data (dict): Location information.

    """
    line_num = file_data.get('line_num')
    file_path = file_data.get('file_path')
    logger_format = file_data.get('logger_format')
    time_args = file_data.get('time_args')
    print(file, line_num, file_path, logger_format, time_args)
    #cfg['sites'][site][location]['line_num'] = line_num + num_of_new_rows


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
                for file, file_data in location_data.items():
                    if file_data.get('automatic_updating'):
                        update_location_file(cfg, output_dir, site, location, file, file_data)

    if 'site' in kwargs:
        site = kwargs.get('site')
        site_data = cfg['sites'][site]
        if not 'location' in kwargs:
            for location, location_data in site_data.items():
                for file, file_data in location_data.items():
                    if file_data.get('automatic_updating'):
                        update_location_file(cfg, output_dir, site, location, file, file_data)
        else:
            location = kwargs.get('location')
            location_data = cfg['sites'][site][location]
            for file, file_data in location_data:
                if file_data.get('automatic_updating'):
                    update_location_file(cfg, output_dir, site, location, file, file_data)

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
                    if args.file_name:
                        info['file_name'] = args.file_name
                    run_system(**info)
                else:
                    run_system()

        elif args.top_parser_name == 'file':
            file_time_to_timestamp(args.input, args.output, args.logger_format, args.time_args, args.raw_time_zone,
                args.target_time_zone)