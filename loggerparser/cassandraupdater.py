#!/usr/bin/env
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import uuid

import pandas

from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import dict_factory

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/cassandra.yaml')


def get_sites(session):
    get_sites_query = "SELECT * FROM sites WHERE bucket=0"
    prepared = session.prepare(get_sites_query)
    rows = session.execute(prepared)

    return rows


def get_locations(session, site_id):
    get_locations_query = "SELECT * FROM locations_by_site WHERE site_id=?"
    prepared = session.prepare(get_locations_query)
    if isinstance(site_id, str):
        site_id = uuid.UUID(site_id)
    rows = session.execute(prepared, (site_id,))

    return rows


def get_location(session, location_id):
    get_location_query = "SELECT * FROM location_info_by_location WHERE location_id=?"
    prepared = session.prepare(get_location_query)
    if isinstance(location_id, str):
        location_id = uuid.UUID(location_id)
    row = session.execute(prepared, (location_id,))

    return row


def get_logs(session, location_id):
    get_logs_query = "SELECT * FROM logs_by_location WHERE location_id=?"
    prepared = session.prepare(get_logs_query)
    if isinstance(location_id, str):
        location_id = uuid.UUID(location_id)
    rows = session.execute(prepared, (location_id,))

    return rows


def get_parameter(session, log_id, parameter_name):
    get_parameter_query = "SELECT * FROM parameter_files_by_log WHERE log_id=? AND parameter_name=?"
    prepared = session.prepare(get_parameter_query)
    if isinstance(log_id, str):
        log_id = uuid.UUID(log_id)
    row = session.execute(prepared, (log_id, parameter_name,))

    return row


def get_parameters(session, log_id):
    get_parameters_query = "SELECT * FROM parameter_files_by_log WHERE log_id=?"
    prepared = session.prepare(get_parameters_query)
    if isinstance(log_id, str):
        log_id = uuid.UUID(log_id)
    rows = session.execute(prepared, (log_id,))

    return rows


def update_parameter_skip_rows(session, log_id, parameter_name, new_value):
    update_skip_rows_query = """
        UPDATE parameter_files_by_log SET skip_rows=? WHERE log_id=? AND parameter_name=?
    """

    prepared = session.prepare(update_skip_rows_query)
    session.execute(prepared, (new_value, log_id, parameter_name,))


def process_parameter(session, log_id, parameter):
    parameter_id = parameter.get('parameter_id')
    parameter_name = parameter.get('parameter_name')
    parameter_type = parameter.get('type')
    file_path = parameter.get('file_path')
    skip_rows = parameter.get('skip_rows')
    insert_readings_query = None

    if parameter_type == 'parameter':
        insert_readings_query = """
            INSERT INTO parameter_readings_by_log (log_id, qc_level, parameter_id, time, value)
                VALUES (?,?,?,?,?)
            """
    elif parameter_type == 'profile':
        insert_readings_query = """
            INSERT INTO profile_readings_by_log
                (log_id, qc_level, parameter_id, time, value, depth) VALUES (?,?,?,?,?,?)
            """

    prepared = session.prepare(insert_readings_query)
    df = pandas.read_csv(file_path, skiprows=range(1, skip_rows), parse_dates=['TMSTAMP'])
    num_of_processed_rows = len(df)

    parameters = []

    for index, row in df.iterrows():
        row_params = (log_id, 0, parameter_id, row['TMSTAMP'].to_pydatetime(), row['VALUE'],)
        try:
            row_params = row_params + (row['DEPTH'],)
        except KeyError:
            pass

        parameters.append(row_params)

    execute_concurrent_with_args(session, prepared, parameters, concurrency=100)

    skip_rows_updated = skip_rows + num_of_processed_rows

    update_parameter_skip_rows(session, log_id, parameter_name, skip_rows_updated)


def process_files(args):
    cfg = utils.load_config(CONFIG_PATH)
    db_settings = cfg['settings']
    keyspace = db_settings.get('keyspace')
    host = db_settings.get('host')

    cluster = Cluster([host])
    session = cluster.connect()
    session.set_keyspace(keyspace)
    session.row_factory = dict_factory

    if args.site:
        if args.location:
            logs = get_logs(session, args.location)
            if args.log:
                log_id = args.log
                if isinstance(log_id, str):
                    log_id = uuid.UUID(log_id)
                if args.parameter:
                    parameter = get_parameter(session, log_id, args.parameter)
                    process_parameter(session, log_id, parameter)
                else:
                    parameters = get_parameters(session, log_id)
                    for parameter in parameters:
                        process_parameter(session, log_id, parameter)
            else:
                for log in logs:
                    log_id = log.get('log_id')
                    parameters = get_parameters(session, log_id)
                    for parameter in parameters:
                        process_parameter(session, log_id, parameter)
        else:
            locations = get_locations(session, args.site)
            for location in locations:
                location_id = location.get('location_id')
                logs = get_logs(session, location_id)
                for log in logs:
                    log_id = log.get('log_id')
                    parameters = get_parameters(session, log_id)
                    for parameter in parameters:
                        process_parameter(session, log_id, parameter)
    else:
        sites = get_sites(session)
        for site in sites:
            site_id = site.get('site_id')
            locations = get_locations(session, site_id)
            for location in locations:
                location_id = location.get('location_id')
                logs = get_logs(session, location_id)
                for log in logs:
                    log_id = log.get('log_id')
                    parameters = get_parameters(session, log_id)
                    for parameter in parameters:
                        process_parameter(session, log_id, parameter)

    cluster.shutdown()


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='CassandraUpdater',
                                     description='Inserts data to the Cassandra database')

    parser.add_argument('-s', '--site', action='store', required=False,
                        dest='site', help='Specific site to convert.')
    parser.add_argument('-l', '--location', action='store', required=False,
                        dest='location', help='Specific location to convert.')
    parser.add_argument('-f', '--log', action='store', required=False,
                        dest='log', help='Specific log to convert.')
    parser.add_argument('-p', '--parameter', action='store', required=False,
                        dest='parameter', help='Specific parameter (name) to convert.')

    args = parser.parse_args()

    if args.parameter:
        if not args.log:
            parser.error("--site, --location and --log are required.")
        else:
            if not args.location:
                parser.error("--site and --location are required.")
            else:
                if not args.site:
                    parser.error("--site and --location are required.")
    else:
        if args.log:
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