from library import Library
import parser

from importlib import reload
import traceback
import os
import sys
from select import select

from argparse import ArgumentParser

import shutil
import parser

def main() -> None:
    arg_parser = ArgumentParser(prog="ytd", description="YouTube downloader and database")

    arg_parser.add_argument("-m",       dest="media_dir",                             help="Path to media directory")
    arg_parser.add_argument("-d",       dest="database_path",                         help="Path to store database file")
    arg_parser.add_argument("-r",       dest="max_resolution",                        help="Maximum video resolution to download | WARNING: Modifies db name and media path as to not cause collisions")
    arg_parser.add_argument("-v",       dest="print_db_log",    action='store_true',  help="Print all database requests")
    arg_parser.add_argument("-q",       dest="quiet",           action='store_true',  help="Quiet mode")
    arg_parser.add_argument("-l",       dest="library",         default='master',     help="Library name")
    arg_parser.add_argument("command",                                                help="Command")
    arg_parser.add_argument("params",   nargs='*',                                    help="Parameters")
    args = arg_parser.parse_args()
    bpath = os.path.expanduser("~/YouTube")
    if args.library == '/': args.library = "master"
    if args.library == 'master' and (args.database_path or args.media_dir):
        print("Error: cannot use master db with custom paths")
        exit(1)
    if args.max_resolution and args.library == "master":
        print("Error: cannot use master db with resolution caps")
        exit(1)
    if args.max_resolution:
        args.library = f"{args.library}.{args.max_resolution}"
    if args.library[0] == '.' and (args.media_dir is None):
        print("Error: cannot use hidden libraries in default media path")
        exit(1)

    lib_db = args.database_path if args.database_path else f"{bpath}/{args.library}.db"

    try:
        shutil.copy(f"{lib_db}.bak", f"{lib_db}.bak2")
    except FileNotFoundError:
        pass
    
    try:
        shutil.copy(lib_db, f"{lib_db}.bak")
    except FileNotFoundError:
        is_first_backup=True

    media_dir = args.media_dir if args.media_dir else f"{bpath}/{args.library}"

    os.makedirs(media_dir, exist_ok=True)

    library = None

    try:
        library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
    except Exception as e:
        try:
            shutil.copy(f"{lib_db}.bak2", f"{lib_db}.bak")
        except FileNotFoundError:
            pass
        print("Error loading database!")
        print(e)
        print(f"Attempting to revert to backup")
        try:
            shutil.copy(f"{lib_db}", f"{lib_db}.err")
            shutil.copy(f"{lib_db}.bak", f"{lib_db}")
        except FileNotFoundError:
            print(f"No backup found, sorry mate :(")
            return
        try:
            library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
        except Exception as f:
            print(f"Uh oh...")
            print(e)
            print(f"Backup either doesn't exist or is also corrupted, sorry mate :(")
            return
        print(f"Backup loaded successfully, corrupted version stored in {lib_db}.err")

    extend_quit = False
    no_extend_quit = False
    previous_cmd = ['']

    parser.run_command(library, args.command, args.params)
    
    library.exit()
    try:
        os.remove(f"{lib_db}.bak2")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    main()
