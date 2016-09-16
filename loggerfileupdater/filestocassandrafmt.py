#!/usr/bin/env
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pytz

import pandas

from loggerfileupdater import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/filestocassandrafmt.yaml')


def process_file(output_dir, site, location, file, file_data, time_zone):

    file_name = file_data.get('name')
    type = file_data.get('type')
    file_path = file_data.get('file_path')
    time_column = file_data.get('time_column')
    skip_rows = file_data.get('skip_rows')
    use_columns = file_data.get('use_columns')
    header_row = file_data.get('header_row')
    to_utc = file_data.get('to_utc')
    file_ext = os.path.splitext(os.path.abspath(file_path))[1]	# Get file extension, e.g. '.dat', '.csv' etc.
    print("Processing file: {0}, {1}".format(file, file_path))

    df = pandas.read_csv(file_path, skiprows=range(header_row + 1, skip_rows), index_col=time_column,
                         parse_dates=[time_column])
    num_of_processed_rows = len(df)

    if num_of_processed_rows > 0:

        try:
            df.index = df.index.tz_localize(tz=time_zone)
        except TypeError:
            print("Datetime already tz-aware")
        if to_utc:
            df.index = df.index.tz_convert(tz=pytz.UTC)


        if type == 'parameter':
            for col in use_columns:
                outfile = os.path.join(os.path.abspath(output_dir), site, location, file_name, col + file_ext)
                header = None
                if not os.path.isfile(outfile):
                    header = ['VALUE']

                os.makedirs(os.path.dirname(outfile), exist_ok=True)    # Create file if it doesn't already exists.

                df.to_csv(outfile, mode='a', columns=[col], header=header,
                      float_format='%.3f', index=True, date_format="%Y-%m-%d %H:%M:%S%z")

        elif type == 'column_profile':
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

        elif type == 'row_profile':
            depth_interval = file_data.get('depth_interval')
            depth_column = file_data.get('depth_column')
            profile_counter_column = file_data.get('profile_counter_column')
            df[depth_column + '_INDEX'] = df[depth_column]
            rounder = lambda x: utils.round_of_rating(x, depth_interval)
            df[depth_column + '_INDEX'] = df[depth_column + '_INDEX'].apply(rounder)
            #df.set_index([profile_counter_column], append=True)
            df = df.reset_index()

            profile_dict = {}

            for idx, series in df.iterrows():
                if series[profile_counter_column] not in profile_dict.keys():
                    profile_dict[series[profile_counter_column]] = series[time_column]

            timestamp_index = []

            for idx, series in df.iterrows():
                timestamp_index.append(profile_dict.get(series[profile_counter_column]))

            df[time_column + '_INDEX'] = timestamp_index

            df = df.set_index([time_column + '_INDEX', profile_counter_column, depth_column + '_INDEX'])

            for col in use_columns:
                outfile = os.path.join(os.path.abspath(output_dir), site, location, file_name, col + file_ext)
                header = None
                if not os.path.isfile(outfile):
                    header = [depth_column, time_column, 'VALUE']

                os.makedirs(os.path.dirname(outfile), exist_ok=True)    # Create file if it doesn't already exists.

                df.to_csv(outfile, mode='a', columns=[depth_column, time_column, col], header=header,
                      float_format='%.3f', index=True, date_format="%Y-%m-%d %H:%M:%S%z")

        return skip_rows + num_of_processed_rows

    return skip_rows


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
                new_skip_rows = process_file(output_dir, args.site, args.location, args.file, file_data, time_zone)
                cfg['sites'][args.site]['locations'][args.location]['files'][args.file]['skip_rows'] = new_skip_rows
            else:
                for f, file_data in files.items():
                    new_skip_rows = process_file(output_dir, args.site, args.location, f, file_data, time_zone)
                    cfg['sites'][args.site]['locations'][args.location]['files'][f]['skip_rows'] = new_skip_rows
        else:
            for l, location_data in site_data.items():
                files = location_data.get('files')
                for f, file_data in files.items():
                    new_skip_rows = process_file(output_dir, args.site, l, f, file_data, time_zone)
                    cfg['sites'][args.site]['locations'][l]['files'][f]['skip_rows'] = new_skip_rows
    else:
        for s, site_data in sites.items():
            for l, location_data in site_data.items():
                files = location_data.get('files')
                for f, file_data in files.items():
                    new_skip_rows = process_file(output_dir, s, l, f, file_data, time_zone)
                    cfg['sites'][s]['locations'][l]['files'][f]['skip_rows'] = new_skip_rows

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