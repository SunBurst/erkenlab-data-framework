#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Misc tools for common datalogger file operations. """

import os

import yaml


class ConfigFileKeyError(KeyError):
    pass


class InvalidRatingValueError(ValueError):
    pass


def load_config(cfg_file):
    """Loads the YAML configuration file into a dictionary.

    Args
    ----
        cfg_file (str): Configuration file's absolute path.

    Returns
    -------
        Configuration file stored in a dictionary.

    """
    with open(cfg_file) as f:
        cfg_dict = yaml.load(f)

    return cfg_dict


def save_config(cfg_file, cfg_mod):
    """Writes data to YAML configuration file.

    Args
    ----
        cfg_file (str): Configuration file's absolute path.
        cfg_mod (dict): The modified configuration file.

    """
    with open(cfg_file, "w+") as f:
        yaml.dump(cfg_mod, f)


def clean_data_output_dir(data_output_dir, *file_types):
    """Delete from the working directory all files of the given type (file extension).

    Args
    ----
        data_output_dir (str): Output directory path.
        file_types (str): File extensions to remove.

    """
    from glob import glob

    for f_type in file_types:
        target = os.path.join(data_output_dir, f_type)
        for f in glob(target):
            os.unlink(f)


def round_of_rating(number, rating):
    """

    Args:
        number (float):
        rating (float):

    Returns:

    """
    if rating == 0.175:
        rating = 8
    elif rating == 0.25:
        rating = 4
    elif rating == 0.5:
        rating = 2
    elif rating == 1.0:
        rating = 1
    else:
        msg = "Invalid data interval. Valid intervals are 0.25, 0.5, 1.0"
        raise InvalidRatingValueError(msg)

    return round(number * rating) / rating
