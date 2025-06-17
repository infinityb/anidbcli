import click
import os
import time
import sys
import json
from base64 import b64encode, b64decode

import pyperclip
import anidbcli.libed2k as libed2k
import anidbcli.anidbconnector as anidbconnector
import anidbcli.output as output
import anidbcli.operations as operations
import traceback
import multiprocessing as mp


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
@click.option("--api-2x", is_flag=True, default=False, help="Use new implementation x")
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
@click.option("--suppress-network-activity", default=False, is_flag=True, help="suppress network activity")
@click.argument("files", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def api(ctx, username, password, apikey, api2, api_2x, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k, suppress_network_activity):
    if api_2x:
        return api_2x_impl(ctx, username, password, apikey, api2, api_2x, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k, suppress_network_activity)
    if api2:
        return api2impl(ctx, username, password, apikey, api2, api_2x, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k, suppress_network_activity)
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
        file_obj["file_path"] = file
        ctx.obj["output"].info("Processing file \"" + file +"\"")

        for operation in pipeline:
            res = operation(file_obj)
            if not res: # Critical error, cannot proceed with pipeline
                break
    conn.close()



def api_2x_impl(ctx, username, password, apikey, api2, api_2x, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k, suppress_network_activity):
    conn = get_connector(apikey, username, password, persistent)
    conn._suppress_network_activity = suppress_network_activity

    pipeline = []
    pipeline.append(operations.GetFileInfoOperation(conn, ctx.obj["output"]))
    
    file_objs_to_process = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        if line == "END":
            break
        if line.startswith("LOOKUP "):
            line = line.removeprefix("LOOKUP ")
            (ed2k, size) = line.split('-')
            size = int(size)
            doc = {
                'ed2k': ed2k,
                'size': size,
            }
            file_objs_to_process.append(doc)
            print(f"register {doc!r}", file=sys.stderr)

    for file_obj in file_objs_to_process:
        for operation in pipeline:
            try:
                res = operation(file_obj)
            except Exception as e:
                if 'file_path' in file_obj:
                    ctx.obj["output"].error(f"error running {operation!r} on {file_obj['file_path']!r}: {e}")
                else:
                    ctx.obj["output"].error(f"error running {operation!r} on file_obj={file_obj!r}: {e}")
                    print(traceback.format_exc(), file=sys.stderr)
                    break
            if not res:  # Critical error, cannot proceed with pipeline
                break
    for file_obj in file_objs_to_process:
        if 'info' in file_obj:
            print(f"SUCC {file_obj['ed2k']}-{file_obj['size']} {json.dumps(file_obj['info'], default=json_serial)}")
    for file_obj in file_objs_to_process:
        if 'info' not in file_obj:
            print(f"FAIL {file_obj['ed2k']}-{file_obj['size']}")
    conn.close()


def api2impl(ctx, username, password, apikey, api2, add, unwatched, rename, files, keep_structure, date_format, delete_empty, link, softlink, persistent, abort, state, show_ed2k, suppress_network_activity):
    if not rename:
        ctx.obj["output"].info("Nothing to do.")
        return
    conn = get_connector(apikey, username, password, persistent)
    conn._suppress_network_activity = suppress_network_activity

    pipeline = []
    pipeline.append(operations.HashOperation(ctx.obj["output"], show_ed2k))
    pipeline.append(operations.GetFileInfoOperation(conn, ctx.obj["output"]))
    pipeline.append(operations.RenameOperation(ctx.obj["output"], rename, date_format, delete_empty, keep_structure, softlink, link, abort))
    
    file_objs_to_process = []
    for file_path in get_files_to_process(files, ctx):
        file_objs_to_process.append({'file_path': file_path})

    # decorate_with_cached(file_objs_to_process)
    # print("{!r}".format(file_objs_to_process))
    # for file_obj in list(map(decorate_with_hash, file_objs_to_process)):
    for file_obj in file_objs_to_process:
        for operation in pipeline:
            try:
                file_obj = decorate_with_hash(file_obj)
                res = operation(file_obj)
            except Exception as e:
                if 'file_path' in file_obj:
                    ctx.obj["output"].error(f"error running {operation!r} on {file_obj['file_path']!r}: {e}")
                else:
                    ctx.obj["output"].error(f"error running {operation!r} on file_obj={file_obj!r}: {e}")
                print(traceback.format_exc(), file=sys.stderr)
                break
            if not res:  # Critical error, cannot proceed with pipeline
                break
    conn.close()


def decorate_with_cached(file_objs):
    values = dict()
    with open(get_ed2k_cache_path(), 'r') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            [file_path_b64, *extras] = json.loads(line)
            values[b64decode(file_path_b64).decode('utf-8')] = extras

    print("", file=sys.stderr)
    for obj in file_objs:
        vv = values.get(obj['file_path'], None)
        print(f"got vv: {vv!r}", file=sys.stderr)
        if vv is not None:
            (ed2k, size) = vv
            file_size = os.path.getsize(obj['file_path'])
            if file_size == size:
                print(f"sz-match ok", file=sys.stderr)
                obj['size'] = size
                obj['ed2k'] = ed2k
                obj['_used_ed2k_precache'] = True
                print(f"obj = {obj!r}", file=sys.stderr)


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


def decorate_with_hash(doc):
    if 'size' in doc and 'ed2k' in doc:
        return doc
    doc = dict(doc)
    if 'size' not in doc:
        doc['size'] = os.path.getsize(doc['file_path'])
    if 'ed2k' not in doc:
        doc['ed2k'] = libed2k.hash_file(doc['file_path'])
    return doc 


def get_persistence_base_path():
    path = os.getenv("APPDATA")
    if path is None: # Unix
        return os.path.join(os.getenv("HOME"), ".anidbcli")
    else:
        return os.path.join(path, "anidbcli")


def get_ed2k_cache_path():
    return os.path.join(get_persistence_base_path(), "ed2k-cache.bin")


def json_serial(obj):
    from datetime import date, datetime
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def main():
    cli(obj={})

if __name__ == "__main__":
    main()
