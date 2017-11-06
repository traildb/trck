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


class ScalarProtoInfo(object):
    def __init__(self, package=None, struct=None, path=None):
        self.package = package if package else "trck"
        self.struct = struct if struct else "Result"
        self.path = path if path else "./Results.proto"
        self.scalar = True

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

    def validate_fields(self, gen_path, program):
        pass


class ProtoInfo(ScalarProtoInfo):
    def __init__(self, package=None, struct=None, row_name_struct=None, path=None):
        self.package = package if package else "trck"
        self.struct = struct if struct else "Results"
        self.path = path if path else "./Results.proto"
        self.row_name_struct = row_name_struct if row_name_struct else ("rows", "Result")
        self.scalar = False

    def to_row_struct(self):
        return "{}__{}__{}".format(
            capitalize(self.package),
            self.struct,
            self.row_name_struct[1],
        )

    def to_row_struct_init(self):
        return "{}__{}__{}__INIT".format(
            snake_case(self.package).upper(),
            snake_case(self.struct).upper(),
            snake_case(self.row_name_struct[1]).upper(),
        )

    def to_row_name(self):
        return self.row_name_struct[0].lower()

    def to_row_struct_descriptor(self):
        return "_{struct}_{row_struct}".format(self.struct.upper(), self.row_name_struct[1].upper())

    def to_struct_descriptor(self):
        return "_{struct}".format(self.struct.upper())

    def get_proto_fields(self, gen_path):
        sys.path.append(gen_path)
        mod = import_from_path(self.path)
        struct_fields = descriptor_fields(getattr(mod, self.to_struct_descriptor()))
        row_fields = descriptor_fields(getattr(mod, self.to_row_struct_descriptor()))
        return struct_fields, row_fields

    def validate_fields(self, gen_path, program):
        struct_fields, row_fields = self.get_proto_fields(gen_path)

        for scalar in program.groupby['vars']:
            name = proto_scalar(scalar)
            if name not in row_fields:
                raise Exception("{} string must be defined in proto file".format(name))
            field_type, field_label = row_fields[name]
            if field_type != TYPE_STRING:
                raise Exception("{} must be a string".format(name))
            if field_label == LABEL_REPEATED:
                raise Exception("{} must not be repeated since it is a scalar".format(name))

        for yield_counter in program.yield_counters:
            name = proto_counter(yield_counter)
            if name not in row_fields:
                raise Exception("{} int64 must be defined in proto file".format(name))
            field_type, field_label = row_fields[name]
            if field_type != TYPE_INT64:
                raise Exception("{} must be int64 type since it is a counter".format(name))
            if field_label == LABEL_REPEATED:
                raise Exception("{} must not be repeated since it is a counter".format(name))

        for yield_set in program.yield_sets:
            name = proto_set(yield_set)
            yield_names = program.get_yield_names("#{}".format(yield_set))
            if len(yield_names) == 1:
                # Expect a repeated string type
                if name not in row_fields:
                    raise Exception("{} repeated string must be defined in proto file".format(name))
                field_type, field_label = row_fields[name]
                if field_type != TYPE_STRING:
                    raise Exception("{} must be a repeated string since it is a set".format(name))
                if field_label != LABEL_REPEATED:
                    raise Exception("{} must be repeated since it is a set".format(name))
            else:
                # Expect a repeated tuple
                if name not in row_fields:
                    raise Exception("{} repeated tuple must be defined in proto file".format(name))
                if field_type != TYPE_MESSAGE:
                    raise Exception("{} must be a repeated tuple since multiple values are yielded to it".format(name))
                if field_label != LABEL_REPEATED:
                    raise Exception("{} must be repeated since it is a set".format(name))


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


def descriptor_fields(desc):
    fields = {}
    for field in desc.fields:
        fields[field.name] = (field.type, field.label)
    return fields


def import_from_path(proto):
    _path, filename = os.path.split(proto)
    name, _extension = os.path.splitext(filename)
    module = "{}_pb2".format(name)
    return importlib.import_module(module)
