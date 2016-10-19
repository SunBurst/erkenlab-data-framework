#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Shared exceptions and functions. """


class UnsupportedValueConversionType(ValueError):
    pass


def update_column_values_generator(data_old, data_new):
    """Iterates one old and one new data set, replacing the modified columns.

    Args
    ----
    data_old (list(OrderedDict)): Data set to update.
    data_new (list(OrderedDict)): Data set to read updates from.

    Returns
    -------
    Updated data set.

    """
    for row_old, row_new in zip(data_old, data_new):
        for name, value_new in row_new.items():
            row_old[name] = value_new
        yield row_old
