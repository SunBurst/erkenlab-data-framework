import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz

from loggerparser import fileformatter, filesplitter, timeconverter, utils

def is_valid_time_zone(parser, arg):
    if not arg in pytz.all_timezones:
        parser.error("Invalid time zone!".format(arg))
    else:
        return arg

def setup_parser():
    """Parses and validates arguments from the command line. """

    parser = argparse.ArgumentParser(prog='LoggerParser', description='LoggerParser desc')
    subparsers = parser.add_subparsers(
        dest='prog_name', description='Program can be run in either automatic system or single file mode.'
    )
    parser_splitfile = subparsers.add_parser(
        'SplitFile', description='Breaks apart nested csv files based on their first element.'
    )
    parser_timeconverter = subparsers.add_parser('TimeConverter')

    subparsers_splitfiles = parser_splitfile.add_subparsers(
        dest='top_parser_name',
        description='Program can be run in either automatic system or single file mode.')
    subparsers_timeconverter = parser_timeconverter.add_subparsers(
        dest='top_parser_name',
        description='Program can be run in either automatic system or single file mode.')
    parser_splitfile_system = subparsers_splitfiles.add_parser(
        'system', help='System mode will process pre-configured files.')
    parser_timeconverter_system = subparsers_timeconverter.add_parser(
        'system', help='System mode will process pre-configured files.')
    parser_splitfiles_modes = parser_splitfile_system.add_subparsers(
        description='Modes sub-commands help', dest='mode_parser_name')
    parser_timeconverter_modes = parser_timeconverter_system.add_subparsers(
        description='Modes sub-commands help', dest='mode_parser_name')
    parser_splitfiles_settings = parser_splitfiles_modes.add_parser('settings', help='Edit system settings.')
    parser_splitfiles_settings.add_argument('-d', '--dir', action='store', help='Set output working directory.')
    parser_splitfiles_settings.add_argument('-c', '--clean', nargs='+', choices=['.*', '*.dat', '*.txt'], action='store',
                                 help='Valid input file types.')
    parser_timeconverter_settings = parser_timeconverter_modes.add_parser('settings', help='Edit system settings.')
    parser_timeconverter_settings.add_argument('-d', '--dir', action='store', help='Set output working directory.')
    parser_timeconverter_settings.add_argument('-c', '--clean', nargs='+', choices=['.*', '*.dat', '*.txt'], action='store',
                                 help='Valid input file types.')

    parser_splitfiles_run = parser_splitfiles_modes.add_parser('run', help='Run automatic system.')
    parser_splitfiles_run.add_argument('-s', '--site', action='store', dest='site', help='Specific site (e.g. lake) to run.')
    parser_splitfiles_run.add_argument('-l', '--location', action='store', dest='location',
                            help='Specific site location (e.g. lake position) to run.')

    parser_timeconverter_run = parser_timeconverter_modes.add_parser('run', help='Run automatic system.')
    parser_timeconverter_run.add_argument('-s', '--site', action='store', dest='site', help='Specific site (e.g. lake) to run.')
    parser_timeconverter_run.add_argument('-l', '--location', action='store', dest='location',
                            help='Specific site location (e.g. lake position) to run.')
    parser_timeconverter_run.add_argument('-f', '--file', action='store', dest='file',
                            help='Specific location file to run.')

    parser_splitfiles_edit = parser_splitfiles_modes.add_parser('edit', help='Edit specific site.')
    parser_splitfiles_edit.add_argument('-p', '--parameter', action='store', nargs='+',
                             choices=['site', 'location', 'line_num', 'file_path'], dest='parameter',
                             required=True, help='Parameter(s) to edit.')
    parser_splitfiles_edit.add_argument('-s', '--site', action='store', dest='site', help='Specific site to edit.')
    parser_splitfiles_edit.add_argument('-l', '--location', action='store', dest='location', help='Specific location to edit.')
    parser_splitfiles_edit.add_argument('-n', '--number', action='store', dest='line_num', help='Set location file line number.')
    parser_splitfiles_edit.add_argument('-f', '--file', action='store', dest='file_path', help='Set location file path.')
    parser_splitfiles_edit.add_argument('-r', '--replacement', action='store', nargs='+', dest='replacement', required=True,
                             help='Replacement value(s).')

    parser_timeconverter_edit = parser_timeconverter_modes.add_parser('edit', help='Edit specific site.')
    parser_timeconverter_edit.add_argument('-p', '--parameter', action='store', nargs='+',
                                           choices=['site', 'location', 'file', 'time_args', 'line_num', 'file_path'],
                                           dest='parameter', required=True, help='Parameter(s) to edit.')
    parser_timeconverter_edit.add_argument('-s', '--site', action='store', dest='site', help='Specific site to edit.')
    parser_timeconverter_edit.add_argument('-l', '--location', action='store', dest='location', help='Specific location to edit.')
    parser_timeconverter_edit.add_argument('-n', '--number', action='store', dest='line_num', help='Set location file line number.')
    parser_timeconverter_edit.add_argument('-fn', '--filename', action='store', dest='file_name', help='Set location file name')
    parser_timeconverter_edit.add_argument('-f', '--file', action='store', dest='file_path', help='Set location file path.')
    parser_timeconverter_edit.add_argument('-t', '--timeargs', action='store', dest='time_args', help='Set file time arguments.')
    parser_timeconverter_edit.add_argument('-r', '--replacement', action='store', nargs='+', dest='replacement', required=True,
                             help='Replacement value(s).')

    parser_splitfiles_updating = parser_splitfiles_modes.add_parser('updating', help='Manage automatic updating.')
    parser_splitfiles_updating.add_argument('-s', '--site', action='store', dest='site', help='Specific site to manage.')
    parser_splitfiles_updating.add_argument('-l', '--location', action='store', dest='location',
                                 help='Specific location to manage.')
    parser_splitfiles_updating.add_argument('--updating', action='store_true', dest='auto_updating', default=False,
                                 help='Turn automatic updating on if given. Turn off otherwise.')

    parser_timeconverter_updating = parser_timeconverter_modes.add_parser('updating', help='Manage automatic updating.')
    parser_timeconverter_updating.add_argument('-s', '--site', action='store', dest='site', help='Specific site to manage.')
    parser_timeconverter_updating.add_argument('-l', '--location', action='store', dest='location',
                                 help='Specific location to manage.')
    parser_timeconverter_updating.add_argument('-f', '--file', action='store', dest='file',
                            help='Specific location file to run.')
    parser_timeconverter_updating.add_argument('--updating', action='store_true', dest='auto_updating', default=False,
                                 help='Turn automatic updating on if given. Turn off otherwise.')

    parser_splitfiles_file = subparsers_splitfiles.add_parser('file', help='File mode will process a single file.')
    parser_splitfiles_file.add_argument('-i', '--input', dest='input', required=True, metavar="FILE",
                                    type=lambda x: utils.is_valid_file(parser, x), help='Input file.')
    parser_splitfiles_file.add_argument('-o', '--output', dest='output', required=True, help='Output directory.')

    parser_timeconverter_file = subparsers_timeconverter.add_parser('file', help='File mode will process a single file.')
    parser_timeconverter_file.add_argument('-i', '--input', dest='input', required=True, metavar="FILE",
                             type=lambda x: utils.is_valid_file(parser, x), help='Input file.')
    parser_timeconverter_file.add_argument('-o', '--output', dest='output', required=True, help='Output directory.')
    parser_timeconverter_file.add_argument('-f', '--format', dest='logger_format',
                                           choices=['campbell legacy', 'campbell modern'], required=True,
                                           help='Logger time format')
    parser_timeconverter_file.add_argument('-t', '--timeargs', dest='time_args', type=int, nargs='+' ,required=True,
                                           help='Time arguments positions (1 2 ... n)')
    parser_timeconverter_file.add_argument(
        '-r', '--fromtz', dest='raw_time_zone', required=True,
        type=lambda x: is_valid_time_zone(parser, x),
        help='Time zone to convert from. Allowed choices are: ' + ', '.join([i for i in pytz.all_timezones]),
    )
    parser_timeconverter_file.add_argument(
        '-z', '--totz', dest='target_time_zone', required=False,
        type=lambda x: is_valid_time_zone(parser, x),
        help='Time zone to convert to (defaults to UTC).'
    )

    args = parser.parse_args()

    if args.prog_name == 'SplitFile':
        if args.top_parser_name == 'system':
            if not args.mode_parser_name:
                parser.error("Mode is required.")
            if args.mode_parser_name == 'edit':
                if len(args.parameter) != len(args.replacement):
                    parser.error("Program expected {0} replacements, "
                                 "got {1}.".format(len(args.parameter), len(args.replacement)))

                for arg in args.parameter:
                    if arg == 'site' and args.site is None:
                        parser.error("--site is required.")
                    elif arg == 'location' and (args.site is None or args.location is None):
                        parser.error("--site and --location are required.")
                    elif arg == 'line_num' and (args.site is None or args.location is None):
                        parser.error("--site, --location and --number are required.")
                    elif arg == 'file_path' and (args.site is None or args.location is None):
                        parser.error("--site, --location and --file are required.")

            if args.mode_parser_name == 'updating':
                if args.location and args.site is None:
                    parser.error("--site is required.")

        filesplitter.process_args(args)

    elif args.prog_name == 'TimeConverter':
        timeconverter.process_args(args)


if __name__=='__main__':
    setup_parser()