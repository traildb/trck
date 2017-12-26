import importlib
import os
import sys

# Descriptor constants from https://github.com/google/protobuf/blob/master/python/google/protobuf/descriptor.py
LABEL_OPTIONAL = 1
LABEL_REQUIRED = 2
LABEL_REPEATED = 3

CPPTYPE_INT32   = 1
CPPTYPE_INT64   = 2
CPPTYPE_UINT32  = 3
CPPTYPE_UINT64  = 4
CPPTYPE_DOUBLE  = 5
CPPTYPE_FLOAT   = 6
CPPTYPE_BOOL    = 7
CPPTYPE_ENUM    = 8
CPPTYPE_STRING  = 9
CPPTYPE_MESSAGE = 10
MAX_CPPTYPE     = 10

TYPE_DOUBLE   = 1
TYPE_FLOAT    = 2
TYPE_INT64    = 3
TYPE_UINT64   = 4
TYPE_INT32    = 5
TYPE_FIXED64  = 6
TYPE_FIXED32  = 7
TYPE_BOOL     = 8
TYPE_STRING   = 9
TYPE_GROUP    = 10
TYPE_MESSAGE  = 11
TYPE_BYTES    = 12
TYPE_UINT32   = 13
TYPE_ENUM     = 14
TYPE_SFIXED32 = 15
TYPE_SFIXED64 = 16
TYPE_SINT32   = 17
TYPE_SINT64   = 18
MAX_TYPE      = 18


INT_TYPES = [
    TYPE_FIXED32,
    TYPE_FIXED64,
    TYPE_SFIXED32,
    TYPE_SFIXED64,
    TYPE_INT32,
    TYPE_INT64,
    TYPE_SINT32,
    TYPE_SINT64,
    TYPE_UINT32,
    TYPE_UINT64,
]

class ProtoInfo(object):
    def __init__(self, package=None, struct=None, path=None):
        self.package = package if package else "trck"
        self.struct = struct if struct else "Result"
        self.path = path if path else "./Results.proto"

    def pb_header(self, basename=False):
        pb_src_file = self.path
        if basename:
            pb_src_file = os.path.basename(pb_src_file)
        return pb_src_file.replace(".proto", ".pb-c.h")

    def pb_src(self, basename=False):
        pb_src_file = self.path
        if basename:
            pb_src_file = os.path.basename(pb_src_file)
        return pb_src_file.replace(".proto", ".pb-c.c")

    def to_struct(self):
        return "{}__{}".format(capitalize(self.package), self.struct)

    def to_struct_init(self):
        return "{}__{}__INIT".format(snake_case(self.package).upper(), snake_case(self.struct).upper())

    def to_get_packed_size(self):
        return "{}__{}__get_packed_size".format(snake_case(self.package).lower(), snake_case(self.struct).lower())

    def to_pack(self):
        return "{}__{}__pack".format(snake_case(self.package).lower(), snake_case(self.struct).lower())

    def to_struct_descriptor(self):
        return "_{struct}".format(
            struct=self.struct.upper())

    def get_proto_fields(self, gen_path):
        sys.path.append(gen_path)
        mod = import_from_path(self.path)
        row_fields = descriptor_fields(getattr(mod, self.to_struct_descriptor()))
        return row_fields

    def validate_fields(self, gen_path, program):
        fields = self.get_proto_fields(gen_path)

        validate_scalars(program, fields)
        validate_counters(program, fields)
        validate_sets(program, fields)


def validate_scalars(program, fields):
    for scalar in program.groupby['vars']:
        name = proto_scalar(scalar)
        if name not in fields:
            raise ValueError("{} string must be defined in proto file".format(name))
        field_type, field_label = fields[name]
        if field_type != TYPE_STRING:
            raise ValueError("{} must be a string".format(name))
        if field_label == LABEL_REPEATED:
            raise ValueError("{} must not be repeated since it is a scalar".format(name))


def validate_counters(program, fields):
    for yield_counter in program.yield_counters:
        name = proto_counter(yield_counter)
        if name not in fields:
            raise ValueError("{} fixed64 must be defined in proto file".format(name))
        field_type, field_label = fields[name]
        if field_type not in INT_TYPES:
            raise ValueError("{} must be int type since it is a counter".format(name))
        if field_label == LABEL_REPEATED:
            raise ValueError("{} must not be repeated since it is a counter".format(name))


def validate_sets(program, fields):
    for yield_set in program.yield_sets:
        name = proto_set(yield_set)
        # Expect a repeated tuple
        if name not in fields:
            raise ValueError("{} repeated Tuple must be defined in proto file".format(name))
        field_type, field_label = fields[name]
        if field_type != TYPE_MESSAGE:
            raise ValueError("{} must be a repeated Tuple since multiple values can be yielded to it".format(name))
        if field_label != LABEL_REPEATED:
            raise ValueError("{} must be repeated since it is a set".format(name))


def capitalize(x):
    return x[0].upper() + x[1:]


def snake_case(x):
    """ ResultTupleExample -> Result_Tuple_Example
        lowerUpper -> Lower_Upper
    """
    y = x[0].upper()
    for i in x[1:]:
        if i.isupper():
            y += "_" + i
        else:
            y += i
    return y


def proto_counter(name):
    if name[0] == '$':
        name = name[1:]
    return "counter_{}".format(name).lower()


def proto_scalar(name):
    if name[0] == '%':
        name = name[1:]
    return "scalar_{}".format(name).lower()


def proto_set(name):
    if name[0] == '#':
        name = name[1:]
    return "set_{}".format(name).lower()


def proto_multiset(name):
    if name[0] == '&':
        name = name[1:]
    return "multiset_{}".format(name).lower()


def proto_hll(name):
    if name[0] == '^':
        name = name[1:]
    return "hll_{}".format(name).lower()


def descriptor_fields(desc):
    fields = {}
    for field in desc.fields:
        fields[field.name] = (field.type, field.label)
    return fields


def import_from_path(proto):
    _path, filename = os.path.split(proto)
    name, _extension = os.path.splitext(filename)
    module = "{}_pb2".format(name.replace('-', '_'))
    return importlib.import_module(module)
