import os

from datetime import datetime

from loggerparser import utils

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'cfg/fileupdate.yaml')


def update_files(source_file, output_dir, line_num, site=None, location=None, frequency=None):

    if not site:
        site = "Unknown site " + datetime.now().strftime("%Y-%m-%d %H %M %S")
    if not location:
        location = "Unknown location " + datetime.now().strftime("%Y-%m-%d %H %M %S")
    if not frequency:
        frequency = "Unknown frequency " + datetime.now().strftime("%Y-%m-%d %H %M %S")

    file_ext = os.path.splitext(os.path.abspath(source_file))[1]	# Get file extension, e.g. '.dat', '.csv' etc.
    f_list = utils.open_file(source_file)

    new_rows = []
    for line_number, line in enumerate(f_list):
        if (line_number >= line_num):
            if (line_num <= line_number):   # If current line is new, process it.
                new_rows.append(line)

    num_of_new_rows = len(new_rows)

    if num_of_new_rows:
        new_file = os.path.join(
            os.path.abspath(output_dir), site, location, frequency + file_ext)	# Construct absolute file path
        os.makedirs(os.path.dirname(new_file), exist_ok=True)    # Create file if it doesn't already exists.
        f_out = open(new_file, 'a')   # Open file in append-only mode.

        for line in new_rows:
            f_out.write(line + "\n")

        f_out.close()

    return num_of_new_rows


def update_line_num(cfg, site, location, frequency, new_line_num):

    cfg['sites'][site][location][frequency]['line_num'] = new_line_num

    return cfg

def run_update(**kwargs):
    cfg = utils.load_config(CONFIG_PATH)
    output_dir = cfg['settings']['output_dir']
    sites = cfg['sites']

    if not kwargs:
        for site, site_data in sites.items():
            for location, location_data in site_data.items():
                for frequency, frequency_data in location_data.items():
                    if frequency_data.get('automatic_updating'):
                        new_rows = update_files(
                            frequency_data.get('file_path'), output_dir, frequency_data.get('line_num'),
                            site, location, frequency)
                        cfg = update_line_num(cfg, site, location, frequency, new_rows)

    if 'site' in kwargs:
        site = kwargs.get('site')
        site_data = cfg['sites'][site]
        if not 'location' in kwargs:
            for location, location_data in site_data.items():
                for frequency, frequency_data in location_data.items():
                    if frequency_data.get('automatic_updating'):
                        new_rows = update_files(
                            frequency_data.get('file_path'), output_dir, frequency_data.get('line_num'), site, location)
                        cfg = update_line_num(cfg, site, location, frequency, new_rows)
        else:
            location = kwargs.get('location')
            location_data = cfg['sites'][site][location]
            if not 'frequency' in kwargs:
                for frequency, frequency_data in location_data.items():
                    new_rows = update_files(
                        frequency_data.get('file_path'), output_dir, frequency_data.get('line_num'), site, location)
                    cfg = update_line_num(cfg, site, location, frequency, new_rows)
            else:
                frequency = kwargs.get('frequency')
                frequency_data = cfg['sites'][site][location][frequency]
                new_rows = update_files(
                    frequency_data.get('file_path'), output_dir, frequency_data.get('line_num'), site, location)
                cfg = update_line_num(cfg, site, location, frequency, new_rows)

    utils.save_config(CONFIG_PATH, cfg)

if __name__=='__main__':
    run_update()