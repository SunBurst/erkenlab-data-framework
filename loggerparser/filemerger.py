#!/usr/bin/python
import ntpath
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from shutil import copyfile

from loggerparser import utils


def append_file(source_data, append_data, headers_line_num):
    """Appends file to the source file.

    Args:
        target_file (list): Source file to append to.
        file (list): File to append.
        headers_line_num (int): The line number (starting counting at 0) of which readings in the file start.

    Returns:
        The source file with the file appended.
    """

    for i, line in enumerate(append_data):
        if headers_line_num >= i:
            source_data[i] = line
        if i > headers_line_num:
            source_data.append(line)

    return source_data


def store_backup(*files):
    """Stores backup files in a directory named 'backup'.

    Args:
        *files: Path to files to backup.
    """

    for file in files:

        dir_name, fname = ntpath.split(file)
        file_name, file_ext = os.path.splitext(fname)
        backup_file = os.path.join(
            os.path.abspath(dir_name), 'backup', file_name + " " +
                                                 datetime.now().strftime("%Y-%m-%d %H %M %S") + file_ext)
        os.makedirs(os.path.dirname(backup_file), exist_ok=True)    # Create file if it doesn't already exist.
        copyfile(file, backup_file)


def process_files(source_file_info, append_file_info, output_file, backup=False,):
    """Reads input files, processes them, stores backups and writes to output.

    Args:
        source_file_info (dict): Source file path and header line number.
        append_file_info (dict): Append file path and header line number.
        output_file (string): Absolute path to output file.
        backup (bool): If True, make a backup folder in each files' directory.
    """

    source_file_path = source_file_info.get('file_path')
    source_file_data = utils.open_file(source_file_path)

    append_file_path = append_file_info.get('file_path')
    append_file_data = utils.open_file(append_file_path)
    append_file_headers_line_num = append_file_info.get('header_line_num')

    source_file_modified = append_file(source_file_data, append_file_data, append_file_headers_line_num)

    if backup:
        store_backup(*[source_file_path, append_file_path])

    os.makedirs(os.path.dirname(output_file), exist_ok=True)    # Create file if it doesn't already exists.

    f_out = open(output_file, 'w', encoding='utf-8')

    for line in source_file_modified:
        f_out.write(line + "\n")

    f_out.close()


def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = utils.FileArgumentParser(prog='FileMerger', description='Merges two or more files.')

    parser.add_argument('-s', '--source', action='store', metavar='FILE', required=True,
                        dest='source_file', help='Source file to append to.')
    parser.add_argument('-a', '--append', action='store', metavar='FILE', required=True,
                        dest='append_file', help='File to append.')
    parser.add_argument('-sn', '--sourceheaders', action='store', type=int,
                        dest='source_file_headers', help='Source file header line number')
    parser.add_argument('-an', '--appendheaders', action='store', type=int,
                        dest='append_file_headers', help='Append file header line number')
    parser.add_argument('-o', '--output', action='store', metavar='FILE', dest='output_file',
                        help='File to write to.')
    parser.add_argument('-b', '--backup', action='store_true', dest='backup', default=False,
                        help='Store original files in a backup directory.')

    args = parser.parse_args()

    source_file_info = {'file_path' : args.source_file, 'header_line_num' : args.source_file_headers}
    append_file_info = {'file_path' : args.append_file, 'header_line_num' : args.append_file_headers}

    process_files(source_file_info, append_file_info, args.backup, args.output_file)

if __name__=='__main__':
    setup_parser()