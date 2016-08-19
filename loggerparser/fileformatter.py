#!/usr/bin/env
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pytz

import pandas

from datetime import datetime

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/fileformatter.yaml')


class RawDataTimeManager(object):
    """Class for converting raw data time formats to timestamp. """

    def __init__(self, time_zone):
        """Class initializer.
        Args: time_zone (string): Raw data pytz time zone.
        """

        self.tz = pytz.timezone(time_zone)

    def parse_custom_format(self, *args):
        pass

    def parse_time(self, *args, to_utc):
        """Converts the given raw data time format to a datetime object (local time zone -> UTC).
        Args:
            args:
        """

        (time_fmt, time_str) = self.parse_custom_format(*args)
        t = time.strptime(time_str, time_fmt)
        dt = datetime.fromtimestamp(time.mktime(t))
        loc_dt = self.tz.localize(dt)
        parsed_dt = loc_dt
        if to_utc:
            utc_dt = loc_dt.astimezone(pytz.utc)
            parsed_dt = utc_dt

        return parsed_dt

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


def process_file(cfg, output_dir, site, location, file, file_data, to_utc, time_zone):

    def parse(time_string):
        raw_tm = CampbellLegacy(time_zone)
        time_args = time_string.split(' ')
        dt = raw_tm.parse_time(*time_args, to_utc=to_utc)

        return dt

    file_name = file_data.get('name')
    file_path = file_data.get('file_path')
    time_columns = file_data.get('time_columns')
    skip_rows = file_data.get('skip_rows')
    data_columns = file_data.get('columns')
    export_columns = file_data.get('export_columns')
    use_columns = file_data.get('use_columns')
    file_ext = os.path.splitext(os.path.abspath(file_path))[1]	# Get file extension, e.g. '.dat', '.csv' etc.
    print("Processing file: {0}, {1}".format(file, file_path))
    df = pandas.read_csv(file_path, skiprows=skip_rows, names=data_columns, usecols=use_columns,
                         parse_dates={'TMSTAMP': time_columns}, date_parser=parse)

    fixed_file = os.path.join(
        os.path.abspath(output_dir), site, location,
        file_name + file_ext)	# Construct absolute file path to subfile.
    os.makedirs(os.path.dirname(fixed_file), exist_ok=True)    # Create file if it doesn't already exists.

    if skip_rows > 0:
        header = None
    else:
        header = export_columns

    df.to_csv(fixed_file, mode='a', columns=export_columns, header=header, date_format="%Y-%m-%d %H:%M:%S%z",
              float_format='%.3f', index=False)

    num_of_processed_rows = len(df)

    cfg['sites'][site][location]['files'][file]['skip_rows'] = skip_rows + num_of_processed_rows


def process_files(args):
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    time_zone = cfg['settings']['time_zone']
    sites = cfg['sites']

    if args.site:
        site_data = sites.get(args.site)
        if args.location:
            location_data = site_data.get(args.location)
            files = location_data.get('files')
            if args.file:
                file_data = files.get(args.file)
                process_file(cfg, output_dir, args.site, args.location, args.file, file_data, args.toutc, time_zone)
            else:
                for f, file_data in files.items():
                    process_file(cfg, output_dir, args.site, args.location, f, file_data, args.toutc, time_zone)
        else:
            for l, location_data in site_data.items():
                files = location_data.get('files')
                for f, file_data in files.items():
                    process_file(cfg, output_dir, args.site, args.location, f, file_data, args.toutc, time_zone)
    else:
        for s, site_data in sites.items():
            for l, location_data in site_data.items():
                files = location_data.get('files')
                for f, file_data in files.items():
                    print(f, file_data)
                    process_file(cfg, output_dir, args.site, args.location, f, file_data, args.toutc, time_zone)

    utils.save_config(CONFIG_PATH, cfg)


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='FileFormatter',
                                     description='Converts Campbell Legacy to Campbell Modern format.')

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to manage.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to manage.')
    parser.add_argument('-f', '--file', action='store', required=False,
                        dest='file', help='Specific file to manage.')
    parser.add_argument('-u', '--toutc', help='Convert time to UTC.', dest='toutc', action='store_true',
                        default=False)

    args = parser.parse_args()

    if args.file:
        if not args.location:
            parser.error("--site and --location are required.")
        else:
            if not args.site:
                parser.error("--site and --location are required.")
    else:
        if args.location and not args.site:
            parser.error("--site and --location are required.")

    process_files(args)

if __name__=='__main__':
    setup_parser()