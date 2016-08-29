#!/usr/bin/env
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import pytz

from datetime import datetime

from pycampbellcr1000 import CR1000

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/loggers.yaml')


def process_file(cfg, output_dir, site, location, location_data):
    conf = cfg
    logger_type = location_data.get('logger')
    time_format = location_data.get('time_format')
    connection_type = location_data.get('connection')
    host_address = location_data.get('host')
    port = location_data.get('port')
    logs = location_data.get('logs')

    device = CR1000.from_url('{0}:{1}:{2}'.format(connection_type, host_address, port))
    device_tables = device.list_tables()

    device_tables_encoded = [i.decode('ascii') for i in device_tables if isinstance(i, bytes)]

    if device_tables_encoded:
        device_tables = device_tables_encoded

    for log_name, log_data in logs.items():
        if log_name in device_tables:
            print(log_name)
            time_zone = log_data.get('time_zone')
            pytz_tz = pytz.timezone(time_zone)
            start_time = log_data.get('start')

            if start_time:
                start_time = datetime.strptime(start_time, time_format)
            else:
                start_time = datetime.fromtimestamp(0)

            stop_time = device.gettime()
            to_utc = log_data.get('to_utc')
            parameters = log_data.get('parameters')

            data = device.get_data(log_name, start_time, stop_time)

            target_file = os.path.join(
                os.path.abspath(output_dir), site, location, log_name + '.csv')	# Construct absolute file path.
            os.makedirs(os.path.dirname(target_file), exist_ok=True)    # Create file if it doesn't already exists.

            with open(target_file, mode='a') as f:
                f.write("%s" % data.to_csv())

            conf['sites'][site]['locations'][location]['logs'][log_name]['start'] = start_time

    return conf


def process_files(args):
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    sites = cfg['sites']

    if args.site:
        site_data = sites.get(args.site)
        locations = site_data.get('locations')
        if args.location:
            location_data = locations.get(args.location)
            process_file(cfg, output_dir, args.site, args.location, location_data)

        else:
            for l, location_data in locations.items():
                process_file(cfg, output_dir, args.site, l, location_data)

    else:
        for s, site_data in sites.items():
            locations = site_data.get('locations')
            for l, location_data in locations.items():
                process_file(cfg, output_dir, s, l, location_data)

    utils.save_config(CONFIG_PATH, cfg)


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='CampbellCollector',
                                     description='Collects and exports data from Campbell CR1000 loggers.')

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to collect.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to collect.')

    args = parser.parse_args()

    if args.location:
        if not args.site:
            parser.error("--site and --location are required.")

    process_files(args)

if __name__=='__main__':
    setup_parser()

