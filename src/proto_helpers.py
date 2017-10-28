import os

def result_struct(proto_name):
	return "Trck__{}".format(proto_name)


def result_struct_init(proto_name):
	return "TRCK__{}__INIT".format(proto_name.upper())


def result_get_packed_size(proto_name):
	return "trck__{}__get_packed_size".format(proto_name.lower())


def result_pack(proto_name):
	return "trck__{}__pack".format(proto_name.lower())


def proto_counter(yield_counter):
	return yield_counter.replace("$", "var_")


def proto_var(var_name):
	return var_name.replace("%", "param_")


def pb_header(proto, basename=False):
	if basename:
		proto = os.path.basename(proto)
	return proto.replace(".proto", ".pb-c.h")


def pb_src(proto, basename=False):
	if basename:
		proto = os.path.basename(proto)
	return proto.replace(".proto", ".pb-c.c")


def proto_name(proto):
	return proto.replace(".proto", "")
