#!/usr/bin/env
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse

import pandas


def process_files(args):

    dfs = []
    pd = pandas.DataFrame()
    columns = []

    for file, header_row in zip(args.input, args.headers):
        df = pandas.read_csv(file, header=header_row, index_col='TIMESTAMP')
        dfs.append(df)
        df_cols = list(df.columns.values)
        for col in df_cols:
            if col not in columns:
                columns.append(col)

    for i, df in enumerate(dfs):
        try:
            if pd.empty:
                merged = df.combine_first(dfs[i+1])
            else:
                merged = pd.combine_first(dfs[i+1])
            pd = merged
        except IndexError:
            pass

    os.makedirs(os.path.dirname(args.output), exist_ok=True)    # Create file if it doesn't already exists.

    pd.to_csv(args.output, mode='a', na_rep="NaN", index=True, columns=columns, float_format='%.3f',
              date_format="%Y-%m-%d %H:%M:%S%z")


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='FileColumnMerger',
                                     description='Merges csv file columns.')

    parser.add_argument('-i', '--input', action='store', nargs='+',
                        dest='input', help='Files to merge.', required=True)
    parser.add_argument('-r','--headers', action='store', type=int, nargs='+',
                        dest='headers', help='Input files header rows, specified in same order as input files. '
                                             'Defaults to 0 (first row).', required=False)
    parser.add_argument('-o', '--output', action='store', required=True,
                        dest='output', help='Output file.')

    args = parser.parse_args()

    for i, file in enumerate(args.input):
        file = file.strip("'")
        if not os.path.isfile(file):
            parser.error("{0} does not exist!".format(file))
        args.input[i] = file

    if not args.headers:
        args.headers = []
    if len(args.input) < len(args.headers):
        parser.error("Too many headers")
    elif len(args.input) > len(args.headers):
        for i, file in enumerate(args.input):
            try:
                args.headers[i]
            except IndexError:
                args.headers.insert(i, 0)

    args.output = args.output.strip("'")

    process_files(args)

if __name__=='__main__':
    setup_parser()