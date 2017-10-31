
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
