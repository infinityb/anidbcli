from collections import namedtuple

class AnidbApiCall(object):
    pass


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


class FileRequest(AnidbApiCall, namedtuple('_FileRequest', ['size', 'ed2k', 'fields'])):
    def serialize(self):
        fmask_fields = 0
        amask_fields = 0
        for f in self.fields:
            if isinstance(f, FileFmaskField):
                fmask_fields &= f.to_bitfield()
            if isinstance(f, FileAmaskField):
                amask_fields &= f.to_bitfield()
        return "FILE size={}&ed2k={}&fmask={:010x}&amask={:08x}".format(
            self.size, self.ed2k, fmask, amask)


class AnimeAmaskField(MaskField, namedtuple('_AnimeAmaskField', ['name', 'byte', 'bit'])):
    KNOWN_FIELDS = []
    BIT_POSITION_LOOKUP = {}
    f = type(object)('AnimeAmaskFieldHolder', (), {})

    def to_sort_tuple(self):
        return (0, 0, self.byte, 7 - self.bit)

    @classmethod
    def register_all(cls, values):
        # Yes, this probably makes it quadratic when called multiple times.  It's probably fine for now.
        for v in values:
            cls.BIT_POSITION_LOOKUP[(v.byte, v.bit)] = v
            setattr(cls.f, v.name, v)
        cls.KNOWN_FIELDS = sorted(cls.KNOWN_FIELDS + values)

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


class FileFmaskField(MaskField, namedtuple('_FileFmaskField', ['byte', 'bit', 'name', 'pytype'])):
    BYTE_LENGTH = 5
    KNOWN_FIELDS = []
    BIT_POSITION_LOOKUP = {}
    f = type(object)('FileFmaskFieldHolder', (), {})

    def to_sort_tuple(self):
        return (1, 0, self.byte, 7 - self.bit)

    @classmethod
    def register_all(cls, values):
        # Yes, this probably makes it quadratic when called multiple times.  It's probably fine for now.
        for v in values:
            cls.BIT_POSITION_LOOKUP[(v.byte, v.bit)] = v
            setattr(cls.f, v.name, v)
        cls.KNOWN_FIELDS = sorted(cls.KNOWN_FIELDS + values)

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
    FileFmaskField(1, 3, 'mylist_id', int),
    FileFmaskField(1, 2, 'other_episodes', None),
    FileFmaskField(1, 1, 'IsDeprecated', int),
    FileFmaskField(1, 0, 'state', int),

    FileFmaskField(2, 7, 'size', int),
    FileFmaskField(2, 6, 'ed2k', str),
    FileFmaskField(2, 5, 'md5', str),
    FileFmaskField(2, 4, 'sha1', str),
    FileFmaskField(2, 3, 'crc32', str),
    # FileFmaskField(2, 2, 'unused', None),
    FileFmaskField(2, 1, 'video_colour_depth', None),
    # FileFmaskField(2, 0, 'reserved'),

    FileFmaskField(3, 7, 'quality', str),
    FileFmaskField(3, 6, 'source', str),
    FileFmaskField(3, 5, 'audio_codec_list', str),
    FileFmaskField(3, 4, 'audio_bitrate_list', int),
    FileFmaskField(3, 3, 'video_codec', str),
    FileFmaskField(3, 2, 'video_bitrate', int),
    FileFmaskField(3, 1, 'video_resolution', str),
    FileFmaskField(3, 0, 'file_type', str),

    FileFmaskField(4, 7, 'dub_language', str),
    FileFmaskField(4, 6, 'sub_language', str),
    FileFmaskField(4, 5, 'length_in_seconds', int),
    FileFmaskField(4, 4, 'description', str),
    FileFmaskField(4, 3, 'aired_date', int),
    # FileFmaskField(4, 2, 'unused'),
    # FileFmaskField(4, 1, 'unused'),
    FileFmaskField(4, 0, 'anidb_file_name', str),

    FileFmaskField(5, 7, 'mylist_state', int),
    FileFmaskField(5, 6, 'mylist_filestate', int),
    FileFmaskField(5, 5, 'mylist_viewed', int),
    FileFmaskField(5, 4, 'mylist_viewdate', int),
    FileFmaskField(5, 3, 'mylist_storage', str),
    FileFmaskField(5, 2, 'mylist_source', str),
    FileFmaskField(5, 1, 'mylist_other', str),
    # FileFmaskField(5, 0, 'unused'),
]);


class FileAmaskField(MaskField, namedtuple('_FileAmaskField', ['byte', 'bit', 'name'])):
    KNOWN_FIELDS = []
    BIT_POSITION_LOOKUP = {}
    f = type(object)('FileAmaskFieldHolder', (), {})
    def to_sort_tuple(self):
        return (1, 1, self.byte, 7 - self.bit)

    @classmethod
    def register_all(cls, values):
        # Yes, this probably makes it quadratic when called multiple times.  It's probably fine for now.
        for v in values:
            cls.BIT_POSITION_LOOKUP[(v.byte, v.bit)] = v
            setattr(cls.f, v.name, v)
        cls.KNOWN_FIELDS = sorted(cls.KNOWN_FIELDS + values)

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
    FileAmaskField(1, 7, 'anime_total_episodes'),
    FileAmaskField(1, 6, 'highest_episode_number'),
    FileAmaskField(1, 5, 'year'),
    FileAmaskField(1, 4, 'type'),
    FileAmaskField(1, 3, 'related_aid_list'),
    FileAmaskField(1, 2, 'related_aid_type'),
    FileAmaskField(1, 1, 'category_list'),

    FileAmaskField(2, 7, 'romaji_name'),
    FileAmaskField(2, 6, 'kanji_name'),
    FileAmaskField(2, 5, 'english_name'),
    FileAmaskField(2, 4, 'other_name'),
    FileAmaskField(2, 3, 'short_name_list'),
    FileAmaskField(2, 2, 'synonym_list'),

    FileAmaskField(3, 7, 'epno'),
    FileAmaskField(3, 6, 'ep_name'),
    FileAmaskField(3, 5, 'ep_romaji_name'),
    FileAmaskField(3, 4, 'ep_kanji_name'),
    FileAmaskField(3, 3, 'episode_rating'),
    FileAmaskField(3, 2, 'episode_vote_count'),

    FileAmaskField(4, 7, 'group_name'),
    FileAmaskField(4, 6, 'group_short_name'),
    FileAmaskField(4, 0, 'date_aid_record_updated'),
])

DEFAULT_API_ENDPOINT_FILE_AMASK = [
    FileAmaskField.f.anime_total_episodes,
    FileAmaskField.f.highest_episode_number,
    FileAmaskField.f.year,
    FileAmaskField.f.type,
    FileAmaskField.f.romaji_name,
    FileAmaskField.f.kanji_name,
    FileAmaskField.f.english_name,
    FileAmaskField.f.other_name,
    FileAmaskField.f.short_name_list,
    FileAmaskField.f.synonym_list,
    FileAmaskField.f.epno,
    FileAmaskField.f.ep_name,
    FileAmaskField.f.ep_romaji_name,
    FileAmaskField.f.ep_kanji_name,
    FileAmaskField.f.group_name,
    FileAmaskField.f.group_short_name,
]

