from abc import ABC, abstractmethod

import os
import datetime
import re
import glob
import errno
import time
import shutil

import anidbcli.libed2k as libed2k 
from anidbcli.protocol import parse_data, FileAmaskField, FileFmaskField, FileRequest

API_ENDPOINT_MYLYST_ADD = "MYLISTADD size=%d&ed2k=%s&viewed=%d&state=%s"
API_ENDPOINT_MYLYST_EDIT = "MYLISTADD size=%d&ed2k=%s&edit=1&viewed=%d&state=%s"

RESULT_FILE = 220
RESULT_MYLIST_ENTRY_ADDED = 210
RESULT_MYLIST_ENTRY_EDITED = 311
RESULT_ALREADY_IN_MYLIST = 310


def IsNullOrWhitespace(s):
        return s is None or s.isspace() or s == ""


class Operation:
    @abstractmethod
    def Process(self, file):
        pass


class MylistAddOperation(Operation):
    def __init__(self, connector, output, state, unwatched):
        self.connector = connector
        self.output = output
        self.state = state 
        if unwatched:
            self.viewed = 0
        else:
            self.viewed = 1

    def Process(self, file):
        try:
            res = self.connector.send_request(API_ENDPOINT_MYLYST_ADD % (file["size"], file["ed2k"], self.viewed, int(self.state)))
            if res.code == RESULT_MYLIST_ENTRY_ADDED:
                self.output.success("Mylist entry added.")
            elif res.code == RESULT_ALREADY_IN_MYLIST:
                self.output.warning("Already in mylist.")
                res = self.connector.send_request(API_ENDPOINT_MYLYST_EDIT % (file["size"], file["ed2k"], self.viewed, int(self.state)))
                if res.code == RESULT_MYLIST_ENTRY_EDITED:
                    self.output.success("Mylist entry state updated.")
                else:
                    self.output.warning("Could not mark as watched.")
            else:
                self.output.error("Couldn't add to mylist: %s" % res["data"])
        except Exception as e:
            self.output.error("Failed to add file to mylist: " + str(e))

        return True


class HashOperation(Operation):
    def __init__(self, output, show_ed2k):
        self.output = output
        self.show_ed2k = show_ed2k

    def Process(self, file):
        try:
            link = libed2k.hash_file(file["path"])
        except Exception as e:
            self.output.error("Failed to generate hash: " + str(e))
            return False
        file["ed2k"] = link
        file["size"] = os.path.getsize(file["path"])
        self.output.success("Generated ed2k link.")
        if self.show_ed2k:
            self.output.info(libed2k.get_ed2k_link(file["path"], file["ed2k"]))
        return True


class GetFileInfoOperation(Operation):
    def __init__(self, connector, output):
        self.connector = connector
        self.output = output


    def Process(self, file):
        request = FileRequest(size=file['size'], ed2k=file['ed2k'], fields=[
            FileFmaskField.f.aid,
            FileFmaskField.f.eid,
            FileFmaskField.f.gid,
            FileFmaskField.f.lid,
            FileFmaskField.f.file_state,
            FileFmaskField.f.size,
            FileFmaskField.f.ed2k,
            FileFmaskField.f.md5,
            FileFmaskField.f.sha1,
            FileFmaskField.f.crc32,
            FileFmaskField.f.color_depth,
            FileFmaskField.f.quality,
            FileFmaskField.f.source,
            FileFmaskField.f.audio_codec,
            FileFmaskField.f.audio_bitrate,
            FileFmaskField.f.video_codec,
            FileFmaskField.f.video_bitrate,
            FileFmaskField.f.resolution,
            FileFmaskField.f.filetype,
            FileFmaskField.f.dub_language,
            FileFmaskField.f.sub_language,
            FileFmaskField.f.length,
            FileFmaskField.f.aired,
            FileFmaskField.f.filename,
            FileAmaskField.f.ep_total,
            FileAmaskField.f.ep_last,
            FileAmaskField.f.year,
            FileAmaskField.f.a_type,
            FileAmaskField.f.a_romaji,
            FileAmaskField.f.a_kanji,
            FileAmaskField.f.a_english,
            FileAmaskField.f.a_other,
            FileAmaskField.f.a_short,
            FileAmaskField.f.a_synonyms,
            FileAmaskField.f.ep_no,
            FileAmaskField.f.ep_english,
            FileAmaskField.f.ep_romaji,
            FileAmaskField.f.ep_kanji,
            FileAmaskField.f.g_name,
            FileAmaskField.f.g_sname,
        ])

        fileinfo = {}
        request_split_max = 2
        while 0 < request_split_max and request:
            request_split_max -= 1
            if not request:
                break
            try:
                res = self.connector.send_request(request)
            except Exception as e:
                self.output.error(f"Failed to get file info: {e}")
                return False
            if res.code != RESULT_FILE:
                self.output.error(f"Failed to get file info: {res!r}")
                return False
            print(f"processing {res!r} -<- {request!r}")
            res.decode_with_query(request, suppress_truncation_error=True)
            fileinfo.update(res.decoded)
            request = request.next_request(res)

        fileinfo["version"] = ""
        fileinfo["censored"] = ""
        
        status = int(fileinfo["file_state"])
        if status & 4: fileinfo["version"] = "v2"
        if status & 8: fileinfo["version"] = "v3"
        if status & 16: fileinfo["version"] = "v4"
        if status & 32: fileinfo["version"] = "v5"
        if status & 64: fileinfo["censored"] = "uncensored"
        if status & 128: fileinfo["censored"] = "censored"

        if IsNullOrWhitespace(fileinfo["ep_english"]):
            fileinfo["ep_english"] = fileinfo["ep_romaji"]
        if IsNullOrWhitespace(fileinfo["a_english"]):
            fileinfo["a_english"] = fileinfo["a_romaji"]

        file["info"] = construct_helper_tags(fileinfo)
        self.output.success("Successfully grabbed file info.")
        return True

class RenameOperation(Operation):
    def __init__(self, output, target_path, date_format, delete_empty, keep_structure, soft_link, hard_link, abort):
        self.output = output
        self.target_path = target_path
        self.date_format = date_format
        self.delete_empty = delete_empty
        self.keep_structure = keep_structure
        self.soft_link = soft_link
        self.hard_link = hard_link
        self.abort = abort
    def Process(self, file):
        try:
            file["info"]["aired"] = file["info"]["aired"].strftime(self.date_format)
        except:
            self.output.warning("Invalid date format, using default one instead.")
            try:
                file["info"]["aired"] = file["info"]["aired"].strftime("%Y-%m-%d")
            except:
                pass  # Invalid input format, leave as is
        target = self.target_path
        for tag in file["info"]:
            if (self.abort and ("%"+tag+"%" in target) and IsNullOrWhitespace(file["info"][tag])):
                self.output.error(f"Rename aborted, {tag!r} is empty.")
                return
            target = target.replace("%"+tag+"%", filename_friendly(file["info"][tag])) # Remove path invalid characters
        target = ' '.join(target.split())  # Replace multiple whitespaces with one
        filename, base_ext = os.path.splitext(file["path"])
        for f in glob.glob(glob.escape(filename) + "*"): # Find subtitle files
            try:
                tmp_tgt = target
                if self.keep_structure:  # Prepend original directory if set
                    tmp_tgt = os.path.join(os.path.dirname(f),target)
                _, file_extension = os.path.splitext(f)
                try:
                    os.makedirs(os.path.dirname(tmp_tgt + file_extension))
                except:
                    pass
                if self.soft_link:
                    os.symlink(f, tmp_tgt + file_extension)
                    self.output.success(f"Created soft link: {tmp_tgt + file_extension!r}")
                elif self.hard_link:
                    os.link(f, tmp_tgt + file_extension)
                    self.output.success(f"Created hard link: {tmp_tgt + file_extension!r}")
                else:
                    shutil.move(f, tmp_tgt + file_extension)
                    self.output.success(f"File renamed to: {tmp_tgt + file_extension!r}")
            except RuntimeError as e:
                self.output.error(f"Failed to rename/link to: {tmp_tgt + file_extension!r}: {e}")
        if self.delete_empty and len(os.listdir(os.path.dirname(file["path"]))) == 0:
            os.removedirs(os.path.dirname(file["path"]))
        file["path"] = target + base_ext


def filename_friendly(input):
    input = f"{input}"
    replace_with_space = ["<", ">", "/", "\\", "*", "|"]
    for i in replace_with_space:
        input = input.replace(i, " ")
    input = input.replace("\"", "'")
    input = input.replace(":","")
    input = input.replace("?","")
    return input


def construct_helper_tags(fileinfo):
    year_list = re.findall('(\d{4})', fileinfo["year"])
    if (len(year_list) > 0):
        fileinfo["year_start"] = year_list[0]
        fileinfo["year_end"] = year_list[-1]
    else:
        fileinfo["year_start"] = fileinfo["year_end"] = fileinfo["year"]

    res_match = re.findall('x(360|480|720|1080|2160)', fileinfo["resolution"])
    if (len(res_match) > 0):
        fileinfo["resolution_abbr"] = res_match[0] + 'p'
    else:
        fileinfo["resolution_abbr"] = fileinfo["resolution"]
    return fileinfo
