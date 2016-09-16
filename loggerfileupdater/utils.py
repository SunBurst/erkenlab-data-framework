import argparse
import os

import yaml


def load_config(cfg_file):
    """Loads the configuration file into memory.

    Args:
        cfg_file (string): Path to configuration file.

    Returns:
        Configuration file stored in memory.

    """
    with open(cfg_file) as f:
        cfg_dict = yaml.load(f)

    return cfg_dict


def save_config(cfg_file, cfg_mod):
    """Write configuration file.

    Args:
        cfg_file (string): Path to configuration file.
        cfg_mod (dict): The modified configuration file stored in memory.

    """
    with open(cfg_file, "w") as f:
        yaml.dump(cfg_mod, f)


def change_dir(dir, cfg_file):
    """Changes working directory by updating the configuration file 'filesplitterold.yaml'.

    Args:
        dir (string): Path to directory.
        cfg_file (string): Path to configuration file.

    """
    new_output_dir = dir
    cfg = load_config(cfg_file)
    try:
        cfg['settings']['output_dir'] = new_output_dir
    except KeyError as e:
        print("Couldn't access key {0}".format(e))
    else:
        save_config(cfg_file, cfg)


def clean_dir(file_types, cfg_file):
    """Delete from the working directory all files of the given type (extension).

    Args:
        file_types (list): List of file extensions.
        cfg_file (string): Path to configuration file.

    """

    from glob import glob
    cfg = load_config(cfg_file)
    output_dir = cfg['settings']['output_dir']
    for f_type in file_types:
        target = output_dir + f_type
        for f in glob(target):
            os.unlink(f)


def open_file(file_path):
    f_in = open(file_path, 'rb')    # Open file to process in read-only binary mode.
    f_list = [line.decode('utf8').strip() for line in f_in]    # Read each line into memory, excluding new line indicators ("\n").
    f_in.close()

    return f_list


class FileArgumentParser(argparse.ArgumentParser):

    def __is_valid_file(self, parser, arg):
        if not os.path.isfile(arg):
            parser.error('The file {} does not exist!'.format(arg))
        else:
            # File exists so return the filename
            return arg

    def __is_valid_directory(self, parser, arg):
        if not os.path.isdir(arg):
            parser.error('The directory {} does not exist!'.format(arg))
        else:
            # File exists so return the directory
            return arg

    def add_argument_with_check(self, *args, **kwargs):
        # Look for your FILE or DIR settings
        if 'metavar' in kwargs and 'type' not in kwargs:
            if kwargs['metavar'] is 'FILE':
                type=lambda x: self.__is_valid_file(self, x)
                kwargs['type'] = type
            elif kwargs['metavar'] is 'DIR':
                type=lambda x: self.__is_valid_directory(self, x)
                kwargs['type'] = type
        self.add_argument(*args, **kwargs)


def round_of_rating(number, rating):
    temp_rating = 0
    if rating == 0.25:
        temp_rating = 4
    elif rating == 0.5:
        temp_rating = 2
    elif rating == 1.0:
        temp_rating = 1
    else:
        raise ValueError("Invalid data interval. Valid intervals are 0.25, 0.5, 1.0")

    return round(number * temp_rating) / temp_rating