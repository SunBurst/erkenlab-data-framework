#!/usr/bin/env
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pytz

import pandas

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/filestocassandrafmt.yaml')


def process_file(cfg, output_dir, site, location, file, file_data, time_zone):

    file_name = file_data.get('name')
    type = file_data.get('type')
    file_path = file_data.get('file_path')
    time_column = file_data.get('time_column')
    skip_rows = file_data.get('skip_rows')
    use_columns = file_data.get('use_columns')
    header_row = file_data.get('header_row')
    file_ext = os.path.splitext(os.path.abspath(file_path))[1]	# Get file extension, e.g. '.dat', '.csv' etc.
    print("Processing file: {0}, {1}".format(file, file_path))

    df = pandas.read_csv(file_path, skiprows=skip_rows, index_col=[time_column], header=header_row, parse_dates=True)
    df.index = df.index.tz_localize(tz=time_zone).tz_convert(tz=pytz.UTC)
    num_of_processed_rows = len(df)

    if type == 'parameter':
        for col in use_columns:
            outfile = os.path.join(os.path.abspath(output_dir), site, location, file_name, col + file_ext)
            header = None
            if not os.path.isfile(outfile):
                header = ['VALUE']

            os.makedirs(os.path.dirname(outfile), exist_ok=True)    # Create file if it doesn't already exists.

            df.to_csv(outfile, mode='a', columns=[col], header=header,
                  float_format='%.3f', index=True, date_format="%Y-%m-%d %H:%M:%S%z")

    elif type == 'profile':
        merged_column_name = file_data.get('merged_column_name')
        depth_columns = file_data.get('depth_columns')

        df = df[use_columns]
        df.columns = depth_columns
        df = df.stack()
        df.index.names = [time_column, 'DEPTH']

        outfile = os.path.join(os.path.abspath(output_dir), site, location, file_name, merged_column_name + file_ext)
        header = None
        if not os.path.isfile(outfile):
            header = ['VALUE']

        os.makedirs(os.path.dirname(outfile), exist_ok=True)    # Create file if it doesn't already exists.

        df.to_csv(outfile, mode='a', header=header,
                  float_format='%.2f', index=True, date_format="%Y-%m-%d %H:%M:%S%z")

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
                process_file(cfg, output_dir, args.site, args.location, args.file, file_data, time_zone)
            else:
                for f, file_data in files.items():
                    process_file(cfg, output_dir, args.site, args.location, f, file_data, time_zone)
        else:
            for l, location_data in site_data.items():
                files = location_data.get('files')
                for f, file_data in files.items():
                    process_file(cfg, output_dir, args.site, l, f, file_data, time_zone)
    else:
        for s, site_data in sites.items():
            for l, location_data in site_data.items():
                files = location_data.get('files')
                for f, file_data in files.items():
                    process_file(cfg, output_dir, s, l, f, file_data, time_zone)

    utils.save_config(CONFIG_PATH, cfg)


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='CassandraConverter',
                                     description='Converts files to Cassandra file formats.')

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to convert.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to convert.')
    parser.add_argument('-f', '--file', action='store', required=False,
                        dest='file', help='Specific file to convert.')

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