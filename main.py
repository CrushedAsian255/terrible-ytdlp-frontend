import os
import shutil

from argparse import ArgumentParser

from library import Library
import parser

def try_copy(src: str, dst: str) -> bool:
    try:
        shutil.copy(src, dst)
    except FileNotFoundError:
        return False
    return True

def main() -> None:
    arg_parser = ArgumentParser(prog="ytd", description="YouTube downloader and database")

    arg_parser.add_argument(
        "-m", help="Path to media directory",
        dest="media_dir"
    )
    arg_parser.add_argument(
        "-d", help="Path to store database file",
        dest="database_path"
    )
    arg_parser.add_argument(
        "-r", help="Maximum video resolution to downloads",
        dest="max_resolution"
    )
    arg_parser.add_argument(
        "-v", help="Print all database requests",
        dest="print_db_log", action='store_true'
    )
    arg_parser.add_argument(
        "-q", help="Quiet mode",
        dest="quiet", action='store_true'
    )
    arg_parser.add_argument(
        "-l", help="Library name",
        dest="library", default='master'
    )
    arg_parser.add_argument(
        "-a", help="Perform auxiliary action (command specific)",
        dest="auxiliary", action='store_true'
    )
    arg_parser.add_argument("command", help="Command")
    arg_parser.add_argument("params",  help="Parameters", nargs='*')

    args = arg_parser.parse_args()
    bpath = os.path.expanduser("~/YouTube")
    if args.library == 'master' and (args.database_path or args.media_dir):
        print("Error: cannot use master db with custom paths")
        return
    if args.max_resolution and args.library == "master":
        print("Error: cannot use master db with resolution caps")
        return
    if args.max_resolution:
        args.library = f"{args.library}.{args.max_resolution}"
    lib_db = args.database_path if args.database_path else f"{bpath}/{args.library}.db"
    try_copy(f"{lib_db}.bak", f"{lib_db}.bak2")
    media_dir: str = args.media_dir if args.media_dir else f"{bpath}/{args.library}"
    try:
        with open(f"{bpath}/{args.library}","r") as f:
            media_dir = f.read()
    except (FileNotFoundError, IsADirectoryError):
        pass
    if args.media_dir:
        media_dir = args.media_dir
        with open(f"{bpath}/{args.library}","w") as f:
            f.write(media_dir)

    os.makedirs(media_dir, exist_ok=True)

    library = None

    try:
        library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
    except Exception as e:
        try_copy(f"{lib_db}.bak2", f"{lib_db}.bak")
        print("Error loading database!")
        print(e)
        print("Attempting to revert to backup")
        shutil.copy(lib_db, f"{lib_db}.err")
        if not try_copy(f"{lib_db}.bak", lib_db):
            print("Sorry, no backup could be found")
            return
        try:
            library = Library(lib_db,media_dir,args.max_resolution,args.print_db_log)
        except Exception as e2:
            print("Uh oh...")
            print(e2)
            print("Backup either doesn't exist or is also corrupted, sorry mate :(")
            return
        print(f"Backup loaded successfully, corrupted version stored in {lib_db}.err")

    parser.run_command(library, args.command, args.params, args.auxiliary)
    library.exit()

    try:
        os.remove(f"{lib_db}.bak2")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    main()
