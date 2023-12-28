import warnings
from collections import namedtuple
from datetime import datetime

QUIRK_ANIMEDESC_LEAVE_SLASH = object()

def _get_query_quirks(query):
    if isinstance(query, AnimeDescRequest):
        return [QUIRK_ANIMEDESC_LEAVE_SLASH]
    return []


def parse_data(raw_data, quirks=[]):
    res = raw_data.split("|")
    for idx, item in enumerate(res):
        item = item.replace("'", "§")  # preseve lists by converting UDP list delimiter ' to § (§ seems unused in AniDB)
        item = item.replace("<br />", "\n")
        if QUIRK_ANIMEDESC_LEAVE_SLASH in quirks and idx == 2:
            # this breaks in ANIMEDESC's bbcode very obviously.
            pass
        else:
            item = item.replace("/", "|")
        item = item.replace("`", "'")
        res[idx] = item
    return res


def _deserialize_field(pytype, field_value):
    if pytype is None:
        return field_value
    if hasattr(pytype, 'deserialize'):
        return pytype.deserialize(field_value)
    if pytype is str:
        return field_value
    if pytype == int:
        return int(field_value)
    if pytype == datetime:
        return datetime.fromtimestamp(int(field_value))
    return field_value


def _assert_code(response, expected_code, req_command):
    if response.code != expected_code:
        raise AnidbApiBadCode(f"bad code for {req_command}",
            code_expected=expected_code,
            code_received=response.code)


class _ListOf:
    def __init__(self, type):
        self._type = type

    def deserialize(self, data):
        return list(_deserialize_field(self._type, x) for x in data.split('§'))


class AnidbApiException(RuntimeError):
    pass


class AnidbApiBadCode(AnidbApiException):
    def __init__(self, *args, **kwargs):
        self.code_expected = kwargs.pop('code_expected')
        self.code_received = kwargs.pop('code_received')
        if not args or not isinstance(args[0], str):
            args = ["incorrect response code received"] + list(args)
        super().__init__(*args, **kwargs)
    
    def _repr_fields(self):
        if self.code_expected is not None:
            yield ('code_expected', self.code_expected)
        if self.code_received is not None:
            yield ('code_received', self.code_received)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)

    def __str__(self):
        return self.__repr__()


class AnidbApiBanned(AnidbApiBadCode):
    pass


class AnidbApiNotFound(AnidbApiBadCode):
    pass


class AnidbResponse(object):
    CODE_LOGIN_FIRST = 501
    CODE_RESULT_FILE = 220
    CODE_RESULT_ANIME_DESCRIPTION = 233
    CODE_RESULT_NO_SUCH_FILE = 320

    def __init__(self, code, data, *, extended=None, body=None, decoded=None):
        self.code = code
        self.data = data
        self.extended = extended
        self.body = body
        self.decoded = decoded

    @classmethod
    def parse(cls, binary):
        (code_text, rest) = binary.split(' ', 1)
        code = int(code_text)
        inst = cls(code, rest)
        parts = rest.split("\n", 1)
        if len(parts) == 2:
            inst.extended = parts[0]
            inst.body = parts[1]
            #if not inst.body.endswith("\n"):
            #    raise RuntimeError('Truncated')
        return inst

    def _repr_fields(self):
        yield ('code', self.code)
        yield ('data', self.data)
        if self.extended is not None:
            yield ('extended', self.extended)
        if self.body is not None:
            yield ('body', self.body)
        if self.decoded is not None:
            yield ('decoded', self.decoded)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)

    def iter_raw_kv(self, query, *, suppress_truncation_error=False):
        if hasattr(query, 'validate_response_has_valid_code'):
            query.validate_response_has_valid_code(self)
        else:
            warnings.warn("query without validate_response_has_valid_code", DeprecationWarning)
        parsed = parse_data(self.body, quirks=_get_query_quirks(query))
        truncation_workaround = slice(None, None)
        if not suppress_truncation_error:
            if len(parsed) != len(query.IMPLICIT_FIELDS) + len(query.fields):
                raise RuntimeError(f'Truncated: {len(parsed)} != {len(query.IMPLICIT_FIELDS) + len(query.fields)}')
        else:
            truncation_workaround = slice(None, len(parsed) - 1)
        for (f, v) in zip(query.fields, parsed[len(query.IMPLICIT_FIELDS):][truncation_workaround]):
            yield f, v

    def decode_with_query(self, query, *, suppress_truncation_error=False):
        if self.decoded is not None:
            return
        if hasattr(query, 'validate_response_has_valid_code'):
            query.validate_response_has_valid_code(self)
        else:
            warnings.warn("query without validate_response_has_valid_code", DeprecationWarning)
        parsed = parse_data(self.body, quirks=_get_query_quirks(query))
        if not suppress_truncation_error:
            if len(parsed) != len(query.IMPLICIT_FIELDS) + len(query.fields):
                raise RuntimeError(f'Truncated: {len(parsed)} != {len(query.IMPLICIT_FIELDS) + len(query.fields)}')
        out = {}
        for ((k, kt), v) in zip(query.IMPLICIT_FIELDS, parsed):
            out[k] = _deserialize_field(kt, v)
        for (f, v) in self.iter_raw_kv(query, suppress_truncation_error=suppress_truncation_error):
            out[f.name] = f.filter_value(v)
        self.decoded = out

    def __getitem__(self, name):
        if name == "code":
            return self.code
        if name == "data":
            return self.data
        raise KeyError(f"{name}")


class AnidbApiCall(object):
    def field_names(self):
        return self.IMPLICIT_FIELDS[0] + [f.name for f in self.fields]


class MaskField(object):
    def __eq__(self, other):
        return self.to_sort_tuple() == other.to_sort_tuple()

    def __ne__(self, other):
        return self.to_sort_tuple() != other.to_sort_tuple()

    def __lt__(self, other):
        return self.to_sort_tuple() < other.to_sort_tuple()

    def __le__(self, other):
        return self.to_sort_tuple() <= other.to_sort_tuple()

    def __ge__(self, other):
        return self.to_sort_tuple() >= other.to_sort_tuple()

    def __gt__(self, other):
        return self.to_sort_tuple() > other.to_sort_tuple()


class FileKeyED2K(object):
    def __init__(self, ed2k, size):
        self.ed2k = ed2k
        self.size = size

    def anidb_props(self):
        yield ('ed2k', self.ed2k)
        yield ('size', self.size)

    def _repr_fields(self):
        yield ('ed2k', self.ed2k)
        yield ('size', self.size)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)


class FileKeyFID(object):
    def __init__(self, fid):
        self.fid = fid

    def __str__(self):
        return 'f{}'.format(self.fid)

    def anidb_props(self):
        yield ('fid', self.fid)

    def _repr_fields(self):
        yield ('fid', self.fid)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)


class FileRequest(AnidbApiCall):
    IMPLICIT_FIELDS = [('fid', int)]
    def __init__(self, *, fields, key=None, size=None, ed2k=None, fid=None):
        self.fields = fields
        if key:
            assert (isinstance(key, FileKeyED2K) or isinstance(key, FileKeyFID))
            self.key = key
        elif fid:
            assert isinstance(fid, int)
            self.key = FileKeyFID(fid=fid)
        elif size and ed2k:
            assert isinstance(ed2k, str)
            assert isinstance(size, int)
            self.key = FileKeyED2K(ed2k=ed2k, size=size)
        else:
            raise Exception("bad key - neither fid, size or ed2k specified: {!r}".format({
                'key': key,
                'size': size,
                'ed2k': ed2k,
                'fid': fid,
            }))

    def serialize(self):
        fmask = 0
        amask = 0
        for f in self.fields:
            if isinstance(f, FileFmaskField):
                fmask |= f.to_bitfield()
            if isinstance(f, FileAmaskField):
                amask |= f.to_bitfield()
        keystr = '&'.join(f'{k}={v}' for (k, v) in self.key.anidb_props())
        return f"FILE {keystr}&fmask={fmask:010X}&amask={amask:08X}"

    def validate_response_has_valid_code(self, response):
        if response.code == AnidbResponse.CODE_RESULT_NO_SUCH_FILE:
            raise AnidbApiNotFound(
                code_received=AnidbResponse.CODE_RESULT_NO_SUCH_FILE,
                code_expected=AnidbResponse.CODE_RESULT_FILE)
        _assert_code(response, AnidbResponse.CODE_RESULT_FILE, "FILE")

    def next_request(self, response):
        return None

    def _repr_fields(self):
        yield ('key', self.key)
        yield ('fields', self.fields)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)


class AnimeAmaskField(MaskField, namedtuple('_AnimeAmaskField', ['name', 'byte', 'bit'])):
    KNOWN_FIELDS = []
    BIT_POSITION_LOOKUP = {}
    f = type(object)('AnimeAmaskFieldHolder', (), {})

    def to_sort_tuple(self):
        return (0, 0, self.byte, 7 - self.bit)

    def __hash__(self):
        return hash((type(self), self.name))

    @classmethod
    def register_all(cls, values):
        # Yes, this probably makes it quadratic when called multiple times.  It's probably fine for now.
        for v in values:
            cls.BIT_POSITION_LOOKUP[(v.byte, v.bit)] = v
            setattr(cls.f, v.name, v)
        cls.KNOWN_FIELDS = sorted(cls.KNOWN_FIELDS + values)

    def filter_value(self, field_value):
        return field_value

    def to_bitfield(self):
        return 1 << 8 * (7 - self.byte) + self.bit

    @classmethod
    def analyze(cls, mask):
        analyzed = []
        for byi in reversed(range(5)):
            for bii in reversed(range(8)):
                chk = 1 << (byi * 8 + bii)
                if chk & mask > 0:
                    v = cls.BIT_POSITION_LOOKUP.get((4 - byi, bii), None)
                    if v is None:
                        v = FileAmaskField(byi + 1, bii, f"unk{chk:08x}")
                    analyzed.append(v)
        return analyzed


class FileFmaskField(MaskField):
    BYTE_LENGTH = 5
    KNOWN_FIELDS = []
    BIT_POSITION_LOOKUP = {}
    f = type(object)('FileFmaskFieldHolder', (), {})

    def __init__(self, byte, bit, name, pytype):
        self.byte = byte
        self.bit = bit
        self.name = name
        self.pytype = pytype

    def __hash__(self):
        return hash((type(self), self.name))

    def to_sort_tuple(self):
        return (1, 0, self.byte, 7 - self.bit)

    @classmethod
    def register_all(cls, values):
        # Yes, this probably makes it quadratic when called multiple times.  It's probably fine for now.
        for v in values:
            cls.BIT_POSITION_LOOKUP[(v.byte, v.bit)] = v
            setattr(cls.f, v.name, v)
        cls.KNOWN_FIELDS = sorted(cls.KNOWN_FIELDS + values)

    def filter_value(self, field_value):
        return _deserialize_field(self.pytype, field_value)

    def to_bitfield(self):
        return 1 << 8 * (self.BYTE_LENGTH - self.byte) + self.bit


    @classmethod
    def analyze(cls, mask):
        analyzed = []
        for byi in reversed(range(cls.BYTE_LENGTH)):
            for bii in reversed(range(8)):
                chk = 1 << (byi * 8 + bii)
                if chk & mask > 0:
                    v = cls.BIT_POSITION_LOOKUP.get((cls.BYTE_LENGTH - byi, bii), None)
                    if v is None:
                        v = FileFmaskField(byi + 1, bii, f"unk{chk:08x}", None)
                    analyzed.append(v)
        return analyzed


FileFmaskField.register_all([
    # FileFmaskField(1, 7, 'unused'),
    FileFmaskField(1, 6, 'aid', int),
    FileFmaskField(1, 5, 'eid', int),
    FileFmaskField(1, 4, 'gid', int),
    FileFmaskField(1, 3, 'lid', int),  # was: mylist_id
    FileFmaskField(1, 2, 'other_episodes', None),
    FileFmaskField(1, 1, 'IsDeprecated', int),
    FileFmaskField(1, 0, 'file_state', int),  # was: state

    FileFmaskField(2, 7, 'size', int),
    FileFmaskField(2, 6, 'ed2k', str),
    FileFmaskField(2, 5, 'md5', str),
    FileFmaskField(2, 4, 'sha1', str),
    FileFmaskField(2, 3, 'crc32', str),
    # FileFmaskField(2, 2, 'unused', None),
    FileFmaskField(2, 1, 'color_depth', None),  # was: video_colour_depth
    # FileFmaskField(2, 0, 'reserved'),

    FileFmaskField(3, 7, 'quality', str),
    FileFmaskField(3, 6, 'source', str),
    FileFmaskField(3, 5, 'audio_codec', _ListOf(str)),  # was: audio_codec_list
    FileFmaskField(3, 4, 'audio_bitrate', _ListOf(int)),  # was: audio_bitrate_list
    FileFmaskField(3, 3, 'video_codec', str),
    FileFmaskField(3, 2, 'video_bitrate', int),
    FileFmaskField(3, 1, 'resolution', str),  # was: video_resolution
    FileFmaskField(3, 0, 'filetype', str),  # was: file_type

    FileFmaskField(4, 7, 'dub_language', str),
    FileFmaskField(4, 6, 'sub_language', str),
    FileFmaskField(4, 5, 'length', int),  # was: length_in_seconds
    FileFmaskField(4, 4, 'description', str),
    FileFmaskField(4, 3, 'aired', datetime),  # was: aired_date
    # FileFmaskField(4, 2, 'unused'),
    # FileFmaskField(4, 1, 'unused'),
    FileFmaskField(4, 0, 'filename', str),  # was: anidb_file_name

    FileFmaskField(5, 7, 'mylist_state', int),
    FileFmaskField(5, 6, 'mylist_filestate', int),
    FileFmaskField(5, 5, 'mylist_viewed', int),
    FileFmaskField(5, 4, 'mylist_viewdate', int),
    FileFmaskField(5, 3, 'mylist_storage', str),
    FileFmaskField(5, 2, 'mylist_source', str),
    FileFmaskField(5, 1, 'mylist_other', str),
    # FileFmaskField(5, 0, 'unused'),
]);


class FileAmaskField(MaskField, namedtuple('_FileAmaskField', ['byte', 'bit', 'name' ,'pytype'])):
    KNOWN_FIELDS = []
    BIT_POSITION_LOOKUP = {}
    f = type(object)('FileAmaskFieldHolder', (), {})
    def to_sort_tuple(self):
        return (1, 1, self.byte, 7 - self.bit)

    def __hash__(self):
        return hash((type(self), self.name))

    @classmethod
    def register_all(cls, values):
        # Yes, this probably makes it quadratic when called multiple times.  It's probably fine for now.
        for v in values:
            cls.BIT_POSITION_LOOKUP[(v.byte, v.bit)] = v
            setattr(cls.f, v.name, v)
        cls.KNOWN_FIELDS = sorted(cls.KNOWN_FIELDS + values)

    def filter_value(self, field_value):
        return _deserialize_field(self.pytype, field_value)

    def to_bitfield(self):
        return 1 << 8 * (4 - self.byte) + self.bit

    @classmethod
    def analyze(cls, mask):
        analyzed = []
        for byi in reversed(range(4)):
            for bii in reversed(range(8)):
                chk = 1 << (byi * 8 + bii)
                if chk & mask > 0:
                    v = cls.BIT_POSITION_LOOKUP.get((4 - byi, bii), None)
                    if v is None:
                        v = FileAmaskField(byi + 1, bii, f"unk{chk:08x}")
                    analyzed.append(v)
        return analyzed


FileAmaskField.register_all([
    FileAmaskField(1, 7, 'ep_total', None),  # was: anime_total_episodes
    FileAmaskField(1, 6, 'ep_last', None),  # was: highest_episode_number
    FileAmaskField(1, 5, 'year', None),
    FileAmaskField(1, 4, 'a_type', None),  # was: type
    FileAmaskField(1, 3, 'related_aid_list', None),
    FileAmaskField(1, 2, 'related_aid_type', None),
    FileAmaskField(1, 1, 'a_categories', None),  # was category_list

    FileAmaskField(2, 7, 'a_romaji', None),  # was: romaji_name
    FileAmaskField(2, 6, 'a_kanji', None),  # was: kanji_name
    FileAmaskField(2, 5, 'a_english', None),  # was: english_name
    FileAmaskField(2, 4, 'a_other', _ListOf(str)),  # was: other_name
    FileAmaskField(2, 3, 'a_short', _ListOf(str)),  # was: short_name_list
    FileAmaskField(2, 2, 'a_synonyms', _ListOf(str)),  # was: synonym_list

    FileAmaskField(3, 7, 'ep_no', None),  # was: epno
    FileAmaskField(3, 6, 'ep_english', None),  # was: ep_name
    FileAmaskField(3, 5, 'ep_romaji', None),  # was: ep_romaji_name
    FileAmaskField(3, 4, 'ep_kanji', None),  # was: ep_kanji_name
    FileAmaskField(3, 3, 'episode_rating', None),
    FileAmaskField(3, 2, 'episode_vote_count', None),

    FileAmaskField(4, 7, 'g_name', None),  # was: group_name
    FileAmaskField(4, 6, 'g_sname', None),  # was: group_short_name
    FileAmaskField(4, 0, 'date_aid_record_updated', None),
])

AnimeAmaskField.register_all([
    AnimeAmaskField('aid', 1, 7),
    AnimeAmaskField('dateflags', 1, 6),
    AnimeAmaskField('year', 1, 5),
    AnimeAmaskField('type', 1, 4),
    AnimeAmaskField('related_aid_list', 1, 3),
    AnimeAmaskField('related_aid_type', 1, 2),

    AnimeAmaskField('romaji_name', 2, 7),
    AnimeAmaskField('kanji_name', 2, 6),
    AnimeAmaskField('english_name', 2, 5),
    AnimeAmaskField('other_name', 2, 4),
    AnimeAmaskField('short_name_list', 2, 3),
    AnimeAmaskField('synonym_list', 2, 2),

    AnimeAmaskField('episodes', 3, 7),
    AnimeAmaskField('highest_episode_number', 3, 6),
    AnimeAmaskField('special_ep_count', 3, 5),
    AnimeAmaskField('air_date', 3, 4),
    AnimeAmaskField('end_date', 3, 3),
    AnimeAmaskField('url', 3, 2),
    AnimeAmaskField('picname', 3, 1),

    AnimeAmaskField('rating', 4, 7),
    AnimeAmaskField('vote_count', 4, 6),
    AnimeAmaskField('temp_rating', 4, 5),
    AnimeAmaskField('temp_vote_count', 4, 4),
    AnimeAmaskField('average_review_rating', 4, 3),
    AnimeAmaskField('review_count', 4, 2),
    AnimeAmaskField('award_list', 4, 1),
    AnimeAmaskField('is_18plus_restricted', 4, 0),

    AnimeAmaskField('ann_id', 5, 6),
    AnimeAmaskField('allcinema_id', 5, 5),
    AnimeAmaskField('animenfo_id', 5, 4),
    AnimeAmaskField('tag_name_list', 5, 3),
    AnimeAmaskField('tag_id_list', 5, 2),
    AnimeAmaskField('tag_weight_list', 5, 1),
    AnimeAmaskField('date_record_updated', 5, 0),

    AnimeAmaskField('character_id_list', 6, 7),

    AnimeAmaskField('specials_count', 7, 7),
    AnimeAmaskField('credits_count', 7, 6),
    AnimeAmaskField('other_count', 7, 5),
    AnimeAmaskField('trailer_count', 7, 4),
    AnimeAmaskField('parody_count', 7, 3),
])


class AnimeDescRequest(AnidbApiCall):
    IMPLICIT_FIELDS = [
        ('cur_part', int),
        ('max_parts', int),
    ]

    def __init__(self, *, aid, part):
        self._aid = aid
        self._part = part
        self.fields = [type('_DynamicField', (object,), {'name': f"content_{self._part}"})()]

    def serialize(self):
        return f"ANIMEDESC aid={self._aid}&part={self._part}"

    def validate_response_has_valid_code(self, response):
        _assert_code(response, AnidbResponse.CODE_RESULT_ANIME_DESCRIPTION, "ANIMEDESC")

    def next_request(self, response):
        response.decode_with_query(self, suppress_truncation_error=True)
        if response.decoded['cur_part'] < response.decoded['cur_part']:
            return AnimeDescRequest(aid=self._aid, part=self._part + 1)

    def _repr_fields(self):
        yield ('aid', self._aid)
        yield ('part', self._part)

    def __repr__(self):
        keys = ', '.join("{}={!r}".format(n, v) for (n, v) in self._repr_fields())
        return "{0.__class__.__module__}.{0.__class__.__name__}({1})".format(self, keys)


