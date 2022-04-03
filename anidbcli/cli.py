import click
import os
import time
import sys
import json
import pyperclip
import anidbcli.libed2k as libed2k
import anidbcli.anidbconnector as anidbconnector
import anidbcli.output as output
import anidbcli.operations as operations
import traceback

@click.group(name="anidbcli")
@click.version_option(version="1.66", prog_name="anidbcli")
@click.option("--recursive", "-r", is_flag=True, default=False, help="Scan folders for files recursively.")
@click.option("--extensions", "-e",  help="List of file extensions separated by , character.")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Display only warnings and errors.")
@click.pass_context
def cli(ctx, recursive, extensions, quiet):
    ctx.obj["recursive"] = recursive
    ctx.obj["extensions"] = None
    ctx.obj["output"] = output.CliOutput(quiet)
    if extensions:
        ext = []
        for i in extensions.split(","):
            i = i.strip()
            i = i.replace(".","")
            ext.append(i)
        ctx.obj["extensions"] = ext


@cli.command(help="Outputs file hashes that can be added manually to anidb.")
@click.option("--clipboard", "-c", is_flag=True, default=False, help="Copy the results to clipboard when finished.")
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def ed2k(ctx , files, clipboard):
    to_process = get_files_to_process(files, ctx)
    links = []
    for file in to_process:
        link = libed2k.get_ed2k_link(file)
        print(link)
        links.append(link)
    if clipboard:
        pyperclip.copy("\n".join(links))
        ctx.obj["output"].success("All links were copied to clipboard.")

@cli.command(help="Utilize the anidb API. You can add files to mylist and/or organize them to directories using "
+ "information obtained from AniDB.")
@click.option('--username', "-u", prompt=True)
@click.option('--password', "-p", prompt=True, hide_input=True)
@click.option('--apikey', "-k")
@click.option("--api2", "-2", is_flag=True, default=False, help="Use new implementation")
@click.option("--add", "-a", is_flag=True, default=False, help="Add files to mylist.")
@click.option("--unwatched", "-U", is_flag=True, default=False, help="Add files to mylist as unwatched. Use with -a flag.")
@click.option("--rename", "-r",  default=None, help="Rename the files according to provided format. See documentation for more info.")
@click.option("--link", "-h", is_flag=True,  default=False, help="Create a hardlink instead of renaming. Should be used with rename parameter.")
@click.option("--softlink", "-l", is_flag=True, default=False, help="Create a symbolic link instead of renaming. Should be used with rename parameter.")
@click.option("--keep-structure", "-s",  default=False, is_flag=True, help="Prepends file original directory path to the new path. See documentation for info.")
@click.option("--date-format", "-d", default="%Y-%m-%d", help="Date format. See documentation for details.")
@click.option("--delete-empty", "-x", default=False, is_flag=True, help="Delete empty folders after moving files.")
@click.option("--persistent", "-t", default=False, is_flag=True, help="Save session info for next invocations with this parameter. (35 minutes session lifetime)")
@click.option("--abort", default=False, is_flag=True, help="Abort if an usable tag is empty.")
@click.option("--state", default=0, help="Specify the file state. (0-4)")
@click.option("--show-ed2k", default=False, is_flag=True, help="Show ed2k link of processed file (while adding or renaming files).")
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def api(ctx, username, password, apikey, api2, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k):
    if api2:
        return api2impl(ctx, username, password, apikey, api2, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k)
    if not add and not rename:
        ctx.obj["output"].info("Nothing to do.")
        return
    try:
        conn = get_connector(apikey, username, password, persistent)
    except Exception as e:
        raise e
        ctx.obj["output"].error(e)
        exit(1)
    pipeline = []
    pipeline.append(operations.HashOperation(ctx.obj["output"], show_ed2k))
    if add:
        pipeline.append(operations.MylistAddOperation(conn, ctx.obj["output"], state, unwatched))
    if rename:
        pipeline.append(operations.GetFileInfoOperation(conn, ctx.obj["output"]))
        pipeline.append(operations.RenameOperation(ctx.obj["output"], rename, date_format, delete_empty, keep_structure, softlink, link, abort))
    to_process = get_files_to_process(files, ctx)
    for file in to_process:
        file_obj = {}
        file_obj["path"] = file
        ctx.obj["output"].info("Processing file \"" + file +"\"")

        for operation in pipeline:
            res = operation(file_obj)
            if not res: # Critical error, cannot proceed with pipeline
                break
    conn.close()


def api2impl(ctx, username, password, apikey, api2, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k):
    if not rename:
        ctx.obj["output"].info("Nothing to do.")
        return
    conn = get_connector(apikey, username, password, persistent)
    linkhive_base_path = '/storage/metameta/anime-links-by-ed2k'
    scan_exempt = set()
    known_keys = set()
    with os.scandir(linkhive_base_path) as scan:
        for entry in scan:
            parts = entry.name.split('-', 2)
            if len(parts) != 2:
                continue
            try:
                link_target = os.readlink(entry.path)
            except OSError as e:
                if e.errno == errno.EINVAL:
                    continue
                else:
                    raise
            (ed2k, size) = parts
            size = int(size, 16)
            known_keys.add((ed2k, size))
            scan_exempt.add(link_target)

    def check_exemption(file):
        if file['path'] in scan_exempt:
            return False
        return True

    def rewrite_hive_path(file):
        fid_natural_key = (file['ed2k'], file['size'])
        linkhive_path = os.path.join(linkhive_base_path, f'{fid_natural_key[0]:>032}-{fid_natural_key[1]:016x}')
        
        link_is_in_hive = fid_natural_key in known_keys
        link_is_self_target = False
        if link_is_in_hive:
            link_target = os.readlink(linkhive_path)

            # cleans up broken link
            if os.path.lexists(link_target) and not os.path.exists(link_target):
                os.unlink(link_target)
                link_is_in_hive = False
            if link_is_in_hive:
                link_is_self_target = link_target == file['path']
        if not link_is_in_hive:
            file['path'] = linkhive_path
            os.symlink(file_path, linkhive_path)
            link_is_self_target = True
        return link_is_self_target

    pipeline = []
    pipeline.append(check_exemption)
    pipeline.append(operations.hash_operation_factory(ctx.obj["output"], show_ed2k))
    pipeline.append(operations.GetFileInfoOperation(conn, ctx.obj["output"]))
    pipeline.append(rewrite_hive_path)
    pipeline.append(operations.RenameOperation(ctx.obj["output"], rename, date_format, delete_empty, keep_structure, softlink, link, abort))

    to_process = get_files_to_process(files, ctx)
    for file_path in to_process:
        file_obj = {}
        file_obj["path"] = file_path
        file_obj["file_path"] = file_path
        ctx.obj["output"].info(f"Processing file {file_path!r}")
        
        for operation in pipeline:
            try:
                res = operation(file_obj)
            except Exception as e:
                ctx.obj["output"].error(f"error running {operation!r} on {file_obj['path']!r}: {e}")
                print(traceback.format_exc(), file=sys.stderr)
                break
            if not res: # Critical error, cannot proceed with pipeline
                break
    conn.close()


def get_connector(apikey, username, password, persistent):
    conn = None
    if persistent:
        path = anidbconnector.get_persistent_file_path()
        if (os.path.exists(path)):
            with open(path, "r") as file:
                lines = file.read()
                data = json.loads(lines)
                if ((time.time() - data["timestamp"]) < 60 * 10):
                    conn = anidbconnector.AnidbConnector.create_from_session(data["session_key"], data["sockaddr"], apikey, data["salt"])
    if (conn != None): return conn
    if apikey:
        conn = anidbconnector.AnidbConnector.create_secure(username, password, apikey)
    else:
        conn = anidbconnector.AnidbConnector.create_plain(username, password)
    return conn


    
def get_files_to_process(files, ctx):
    to_process = []
    for file in files:
        if os.path.isfile(file):
            to_process.append(file)
        elif ctx.obj["recursive"]:
            for folder, _, files in os.walk(file):
                for filename in files:
                    to_process.append(os.path.join(folder,filename))
    ret = []
    for f in to_process:
        if (check_extension(f, ctx.obj["extensions"])):
            ret.append(f)
    return ret

def check_extension(path, extensions):
    if not extensions:
        return True
    else:
        _, file_extension = os.path.splitext(path)
        return file_extension.replace(".", "") in extensions


def main():
    cli(obj={})

if __name__ == "__main__":
    main()
