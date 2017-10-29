import os

def outer_struct(proto_info):
	return "{}__{}".format(proto_info[0], proto_info[1])


def outer_struct_init(proto_info):
	return "{}__{}__INIT".format(proto_info[0].upper(), proto_info[1].upper())


def inner_struct(proto_info):
	return "{}__{}__{}".format(
		proto_info[0],
		proto_info[1],
		proto_info[3],
	)

def inner_struct_init(proto_info):
	return "{}__{}__{}__INIT".format(
		proto_info[0].upper(),
		proto_info[1].upper(),
		proto_info[3].upper(),
	)

def get_packed_size(proto_info):
	return "{}__{}__get_packed_size".format(proto_info[0].lower(), proto_info[1].lower())


def pack(proto_info):
	return "{}__{}__pack".format(proto_info[0].lower(), proto_info[1].lower())


def pb_header(proto_info, basename=False):
	pb_src_file = proto_info[4]
	if basename:
		pb_src_file = os.path.basename(pb_src_file)
	return pb_src_file.replace(".proto", ".pb-c.h")


def pb_src(proto_info, basename=False):
	pb_src_file = proto_info[4]
	if basename:
		pb_src_file = os.path.basename(pb_src_file)
	return pb_src_file.replace(".proto", ".pb-c.c")


def proto_counter(yield_counter):
	return yield_counter.replace("$", "var_")


def proto_var(var_name):
	return var_name.replace("%", "param_")
