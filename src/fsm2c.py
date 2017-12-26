import re
import sys
import json
import hashlib

import proto_helpers as ph


EXPIRES_NEVER = 'UINT64_MAX'


class BRACES(object):
    def __init__(self, gen, head=None, **context):
        self.gen = gen
        self.head = head
        self.context = context

    def __enter__(self):
        if self.head:
            self.gen.o(self.head)
        self.gen.o("{")
        self.gen.indent(self.context)

    def __exit__(self, type, value, tb):
        self.gen.dedent()
        self.gen.o("}")


class DEBUG(object):
    def __init__(self, gen):
        self.gen = gen

    def __enter__(self):
        self.gen.o("#if DEBUG")

    def __exit__(self, type, value, tb):
        self.gen.o("#endif")


class Gen(object):
    def __init__(self, outfile):
        self.outfile = outfile
        self.indent_level = 0
        self.context = [{}]

    def co(self, code):
        """
        Like self.o but formats the current context into code
        """

        # Combine all frames of context into a single dictionary
        # overwriting kv pairs from lower frames
        flattened_context = {}
        for frame in self.context:
            flattened_context.update(frame)

        code = code.format(**flattened_context)
        self.o(code)

    def o(self, code):
        line = "{indent}{code}\n".format(
            indent=" " * 4 * self.indent_level,
            code=code)
        self.outfile.write(line)

    def indent(self, context):
        self.indent_level += 1
        # Add new frame to be deleted
        # when exiting current block
        self.context.append(context)

    def dedent(self):
        self.indent_level -= 1
        # Delete top level frame
        self.context.pop()

    def push_context(self, key, var):
        """
        Add kv pair to topmost frame
        """
        self.context[-1][key] = var



def escape_var_name(n):
    return re.sub('[^a-zA-Z0-9_]', lambda x: x.group(0).encode('hex'), n)


def var_type(n):
    if n.startswith('#'):
        return 'set'
    elif n.startswith('&'):
        return 'multiset'
    elif n.startswith('%'):
        return 'scalar'
    elif n.startswith('@'):
        return 'composite'
    elif n.startswith('$'):
        return 'scalar'
    elif n.startswith('^'):
        return 'hll'
    else:
        assert(not n)


def is_variable(n):
    if n == '':
        return False
    return n[0] in '#&%$@^'


def strip_type(v):
    return v[1:]


def compile_clause_condition_check(g, ri, ci, c, succ, fail):
    with BRACES(g):
        g.o("bool r = true;")
        for field, conditions in sorted(c["attrs"].items(), reverse=True, key=lambda v: v[1]):
            if field != 'timestamp':

                for expr in conditions:
                    if is_variable(expr):
                        if var_type(expr) == 'scalar':
                            with BRACES(g, "if(r)"):
                                g.o("ctx_update_stats(ctx, GROUPBY_USED);")
                                g.o("r = r && (item_get_value_id(item, ids->key_%s) == ids->var_%s);" % (field, strip_type(expr)))
                        elif var_type(expr) == 'set' or var_type(expr) == 'multiset':
                            with BRACES(g, "if(r)"):
                                g.o("ctx_update_stats(ctx, GROUPBY_USED);")
                                g.o("r = r && (set_contains(ids->var_%s, item_get_value_id(item, ids->key_%s)));" % (strip_type(expr), field))
                    else:
                        g.o("if (r) r = r && (item_get_value_id(item, ids->key_%s) == ids->value_%s_%s);" % (field, field, escape_var_name(expr)))

            else:   # timestamp:
                for expr in conditions:
                    with BRACES(g, "if(r)"):
                        expr = expr.strip()

                        if expr[0].isdigit():
                            op = '=='
                            value = expr
                        else:
                            m = re.match('((>=)|(<=)|(==)|(<)|(>))(.+)', expr)
                            assert(m)
                            op = m.group(1)
                            value = m.group(7)

                            if value.startswith('%'):
                                g.o("ctx_update_stats(ctx, GROUPBY_USED);")
                                value = "ids->var_%s" % (value.lstrip('%'))

                        g.o("r = r && (timestamp %s %s);" % (op, value))
        g.o(";")

        if c["attrs"]:
            with DEBUG(g):
                g.o("""DBG_PRINTF("event\\n    ts=%" PRIu64 "\\n", timestamp);""")
                for field, conditions in c["attrs"].items():
                    if field == "timestamp":
                        continue

                    for expr in conditions:

                        if is_variable(expr) and var_type(expr) == 'scalar':
                            with BRACES(g, "/* field '{}' */".format(field)):
                                g.o("int len;")
                                g.o("const char *v = ctx_get_item_value(ctx, item, ids->key_{}, &len);".format(field))
                                g.o("""DBG_PRINTF("    {field}='%.*s' (key id %d, value id %d)\\n", len, v, ids->key_{field}, ids->var_{expr});""".format(field=field, expr=strip_type(expr)))
                        elif is_variable(expr) and var_type(expr) == 'set':
                            with BRACES(g, "/* field '{}' */".format(field)):
                                g.o("int len;")
                                g.o("const char *v = ctx_get_item_value(ctx, item, ids->key_{}, &len);".format(field))
                                g.o("""DBG_PRINTF("    {field}='%.*s' (key id %d)\\n", len, v, ids->key_{field});""".format(field=field))
                        else:
                            with BRACES(g, "/* field '{}' */".format(field)):
                                g.o("int len;")
                                g.o("const char *v = ctx_get_item_value(ctx, item, ids->key_{}, &len);".format(field))
                                g.o("int value_id = item_get_value_id(item, ids->key_{});".format(field))
                                g.o("""DBG_PRINTF("    {field}='%.*s' (key id %d, value id %d)\\n", len, v, ids->key_{field}, value_id);""".format(field=field))

                g.o("""DBG_PRINTF("predicate\\n")""")
                for field, conditions in c["attrs"].items():
                    if field == "timestamp":
                        continue

                    for expr in conditions:
                        if is_variable(expr) and var_type(expr) == 'scalar':
                            with BRACES(g, "/* field '{}' */".format(field)):
                                g.o("int len;")
                                g.o("const char *v = ctx_get_item_value(ctx, item, ids->key_{}, &len);".format(field))
                                g.o("""DBG_PRINTF("    {field}='%.*s' (field id %d, value id %d)\\n", len, v, ids->key_{field}, ids->var_{expr});""".format(field=field, expr=strip_type(expr)))
                        elif is_variable(expr) and var_type(expr) == 'set':
                            g.o("""DBG_PRINTF("    {field}=..set.. (field id %d)\\n", ids->key_{field});""".format(field=field))
                        else:
                            with BRACES(g, "/* field '{}' */".format(field)):
                                g.o("int len;")
                                g.o("const char *v = ctx_get_item_value(ctx, item, ids->key_{field}, &len);".format(field=field))
                                g.o("""DBG_PRINTF("    {field}='%.*s' (field id %d, value id %d)\\n", len, v, ids->key_{field}, ids->value_{field}_{expr});""".format(field=field, expr=escape_var_name(expr)))

                g.o("""DBG_PRINTF("    matched? %s\\n", r ? "yes" : "no");""")



        if c.get("op") == "not":
            g.o("if (!r) goto %s; else goto %s;" % (succ, fail))
        else:
            g.o("if (r) goto %s; else goto %s;" % (succ, fail))


def enter_rule(g, ri, program):
    if program.rules[ri].get("outer"):
        g.o('DBG_PRINTF("entering outer window at \\"%s\\"\\n");' % program.get_rule_name(ri))
        with BRACES(g, "for (int i = 0; i < sizeof(state->outers) / sizeof(outer_info_t); i++)"):
            with BRACES(g, "if (state->outers[i].id == -1)"):
                g.o("state->outers[i].id = %d;" % ri)
                g.o("state->outers[i+1].id = -1;")
                if program.get_rule_window_duration(ri) is not None:
                    with BRACES(g, "if (state->window_expires > 0)"):
                        g.o("state->outers[i].window_expires = MIN(timestamp, state->window_expires) + %d;" % program.get_rule_window_duration(ri))
                    with BRACES(g, "else"):
                        g.o("state->outers[i].window_expires = timestamp + %d;" % program.get_rule_window_duration(ri))
                else:
                    g.o("state->outers[i].window_expires = %s;" % EXPIRES_NEVER)
                g.o("break;")
    else:
        if program.get_rule_window_duration(ri) is not None:
            with BRACES(g, "if (state->window_expires > 0)"):
                g.o("state->window_expires = MIN(timestamp, state->window_expires) + %s;" % (program.get_rule_window_duration(ri)))
            with BRACES(g, "else"):
                g.o("state->window_expires = timestamp + %s;" % (program.get_rule_window_duration(ri)))
        else:
            g.o("state->window_expires = %s;" % EXPIRES_NEVER)


def compile_yield_term(g, term, program, current_rule_id, _val, _pval, _len, _type):
    kind = term.get('_k')

    if kind == 'window_ref':
        window = term.get('ref')
        if window is None:
            duration = program.get_rule_window_duration(current_rule_id)
            if duration is None:
                raise Exception('Cannot yield window start timestamp when window is infinite')

            # convert start timestamp to a string and store it to _val
            g.o('snprintf(%(_val)s, sizeof(%(_val)s)/sizeof(%(_val)s[0]), "%%" PRIu64, state->window_expires - %(duration)d);' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())
        else:
            window_id = program.get_rule_id_by_name(window)
            duration = program.get_rule_window_duration(window_id)
            if duration is None:
                raise Exception('Cannot yield window start timestamp when window is infinite')

            pos = program.get_rule_window_block_stack_pos(current_rule_id, window_id)

            g.o('snprintf(%(_val)s, sizeof(%(_val)s)/sizeof(%(_val)s[0]), "%%" PRIu64, state->outers[%(pos)d].window_expires - %(duration)d);' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())

    elif kind == 'field':
        field_name = term['name']

        if field_name == 'cookie':
            g.o("ctx_get_cookie(ctx, %(_val)s);" % locals())
            g.o('%(_len)s = 16;' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_BYTES;' % locals())

        elif field_name == 'timestamp':
            g.o('snprintf(%(_val)s, sizeof(%(_val)s)/sizeof(%(_val)s[0]), "%%" PRIu64, item_get_timestamp(i));' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())

        elif field_name == 'cookie_timestamp_filter_end':
            g.o('snprintf(%(_val)s, sizeof(%(_val)s)/sizeof(%(_val)s[0]), "%%" PRIu64, ctx_get_cookie_timestamp_filter_end(ctx));' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())

        elif field_name == 'cookie_timestamp_filter_start':
            g.o('snprintf(%(_val)s, sizeof(%(_val)s)/sizeof(%(_val)s[0]), "%%" PRIu64, ctx_get_cookie_timestamp_filter_start(ctx));' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())

        else:
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())
            with BRACES(g, "if (ids->key_%s != -1)" % field_name):
                g.o("const char *v = ctx_get_item_value(ctx, i, ids->key_%s, &%s);" % (field_name, _len))
                g.o("memcpy(%(_val)s, v, %(_len)s < sizeof(%(_val)s) ? %(_len)s : sizeof(%(_val)s));" % locals())
    elif kind == 'literal':
        literal_val = term['value']
        if isinstance(literal_val, int):
            g.o('snprintf(%(_val)s, sizeof(%(_val)s)/sizeof(%(_val)s[0]), "%%ld", %(literal_val)dl);' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())
        else:
            g.o('strncpy(%(_val)s, \"%(literal_val)s\", sizeof(%(_val)s)/sizeof(%(_val)s[0])-1);' % locals())
            g.o('%(_len)s = strlen(%(_val)s);' % locals())
            g.o('%(_val)s[%(_len)s] = 0; // add 0 after strncpy' % locals())
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())
    elif kind == 'param':
            scalar_name = strip_type(term['name'])
            with BRACES(g):
                g.o('%(_len)s = ids->varstrlen_%(scalar_name)s;' % locals())
                g.o('%(_pval)s = ids->varstr_%(scalar_name)s;' % locals())
                g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())
    elif kind == 'fcall':
        with BRACES(g):
            slug = hashlib.sha1(repr(term)).hexdigest()[:6]
            args = [_val, "sizeof(%s)/sizeof(%s[0])" % (_val, _val)]
            for narg, arg in enumerate(term['args']):
                arg_val = 'arg_%s_%d' % (slug, narg)
                arg_buf = 'argbuf_%s_%d' % (slug, narg)
                arg_len = 'len_%s_%d' % (slug, narg)
                arg_type = 'type_%s_%d' % (slug, narg)

                g.o("char %s[256] = \"\";" % arg_buf)
                g.o("char *%s = %s;" % (arg_val, arg_buf))
                g.o("int %s = 0;" % arg_len)
                g.o("int %s __attribute__((unused)) = 0;" % arg_type)

                compile_yield_term(g, arg, program, current_rule_id, arg_buf, arg_val, arg_len, arg_type)
                args.append(arg_val)
                args.append(arg_len)
            g.o('%s = %s(%s);' % (_len, term['name'], ','.join(args)))
            g.o('%(_type)s = TUPLE_ITEM_TYPE_STRING;' % locals())


def compile_yield(g, c, program, current_rule_id):
    if c.get("yield", None):
        g.o('DBG_PRINTF("yield %s\\n");' % repr(c['yield']).replace('%', '%%'))
        g.o("ctx_update_stats(ctx, RESULT_UPDATED);")
        for _yield in c['yield']:
            var = _yield['dst']
            if var_type(var) == 'scalar':
                g.o("results->%s += 1;" % strip_type(var))
            elif var_type(var) == 'set' or var_type(var) == 'multiset' or var_type(var) == 'hll':
                _tuple = _yield['src']

                with BRACES(g):
                    g.o("string_tuple_t tuple;")
                    g.o("string_tuple_init(&tuple);")
                    g.o("item_t i = ctx_get_item(ctx);")
                    for elem in _tuple:
                        with BRACES(g):
                            g.o("char buf[256] = \"\";")
                            g.o("char *val = buf;")
                            g.o("int len = 0;")
                            g.o("int type = 0;")
                            compile_yield_term(g, elem, program, current_rule_id, 'buf', 'val', 'len', 'type')
                            g.o("string_tuple_append(val, len, type, &tuple);")
                    if var_type(var) == 'set':
                        g.o("set_insert(&results->set_%s, &tuple);" % strip_type(var))
                    elif var_type(var) == 'multiset':
                        g.o("mset_insert(&results->mset_%s, &tuple);" % strip_type(var))
                    elif var_type(var) == 'hll':
                        g.o("results->hll_%s = hll_insert(results->hll_%s, &tuple);" % (strip_type(var), strip_type(var)))
                    else:
                        raise Exception('Bad yield: %s' % var)
            else:
                raise Exception('Bad yield: %s' % var)


class Action:
    def __init__(self, type, label):
        self.type, self.label = type, label

    def __unicode__(self):
        return "<type=%s label=%s>" % (self.type, self.label)


def parse_action(s):
    m = re.match(r'(?P<type>(restart-from-(here|next|start))|break|repeat|stop|quit)(\((?P<label>\w+)\))?', s)
    if m is None:
        raise Exception("Unknown action: %s" % s)
    return Action(type=m.group('type'), label=m.group('label'))


def balance_window_rules(g, src_rule, dst_rule, program):
    if not program.has_window_rules:
        return

    # may need to exit window rules when jumping from src to dst
    # if src_rule is within an window block and dst is not.
    #
    # these jumps are only defined when dst window rules are prefix
    # for src window rules, ie imagine a program like this:
    # A B{C D} E F{G H}
    #
    # you can transition from C->A or C->D but C->G transition semantics are undefined
    if len(program.rule_windows[src_rule]) < len(program.rule_windows[dst_rule]):
        raise Exception("Invalid transition: jumping between unrelated window blocks  %s->%s" % (src_rule, dst_rule))
    if program.rule_windows[src_rule][:len(program.rule_windows[dst_rule])] != program.rule_windows[dst_rule]:
        raise Exception("Invalid transition: jumping between unrelated window blocks %s->%s" % (src_rule, dst_rule))

    g.o("state->outers[%d].id = -1;" % len(program.rule_windows[dst_rule]))
    g.o("state->outers[%d].window_expires = 0;" % len(program.rule_windows[dst_rule]))


def compile_clause_action(g, ri, ci, c, program):
    action = parse_action(c.get("action", "restart-from-here"))
    with BRACES(g):
        g.o('DBG_PRINTF("exec rule \\"%s\\" clause %s (ts=%%" PRIu64 " window_expires=%%" PRIu64 ")\\n", timestamp, state->window_expires);' % (program.get_rule_name(ri), ci))
        compile_yield(g, c, program, ri)
        if action.type == "break":
            # omitted error checks
            g.o("ctx_advance(ctx);")
            g.o('DBG_PRINTF("advance to pos %" PRId64 "\\n", ctx_get_position(ctx));')
            balance_window_rules(g, ri, ri+1, program)
            g.o("goto RULE_START_r%d;" % (ri+1))
        elif action.type == "repeat":
            g.o("ctx_advance(ctx);")
            g.o('DBG_PRINTF("advance to pos %" PRId64 "\\n", ctx_get_position(ctx));')
            # if action.type is repeat, but outer expires, shouldn't that be an error?
            g.o("goto CONTINUE_r%d;" % ri)
        elif action.type == "restart-from-here":
            g.o('DBG_PRINTF("restarting from current event at \\"%s\\" pos=%%" PRId64 "\\n", ctx_get_position(ctx));' % program.get_rule_name( int(action.label or ri)))
            balance_window_rules(g, ri, int(action.label or 0), program)
            g.o("goto RULE_START_r%d;" % int(action.label or 0))
        elif action.type == "restart-from-next":
            g.o("ctx_advance(ctx);")
            g.o('DBG_PRINTF("restarting from next event at \\"%s\\" pos=%%" PRId64 "\\n", ctx_get_position(ctx));' % program.get_rule_name( int(action.label or ri)))
            g.o('DBG_PRINTF("advance to pos %"PRId64"\\n", ctx_get_position(ctx));')
            balance_window_rules(g, ri, int(action.label or 0), program)
            g.o("goto RULE_START_r%d;" % int(action.label or 0))
        elif action.type == "restart-from-start":
            raise Exception("'restart-from-start' not supported")
        elif action.type == "stop" or action.type == 'quit':
            g.o("abort = 1;")
            g.o("state->ri = -1;")
            g.o("goto STOP;")
        else:
            assert("unknown action.type %s" % action.type)


def compile_clause(g, ri, ci, c, succ, fail, program):
    with BRACES(g):
        compile_clause_condition_check(g, ri, ci, c, succ="CLAUSE_%s_%s_success" % (ri, ci), fail=fail)
        g.o("CLAUSE_%s_%s_success:" % (ri, ci))
        with BRACES(g):
            compile_clause_action(g, ri, ci, c, program)


class Program:
    def __init__(self, rules, groupby):
        self.rules = rules
        self.groupby = groupby

        # Ids of 'window' rules
        self.window_rule_ids = []

        # Sets of result variables of both types.
        self.yield_counters = set()
        self.yield_sets = set()
        self.yield_multisets = set()
        self.yield_hlls = set()

        # list of (name, nargs) for external functions
        self.external_functions = []

        # maps field id to a set of values used within the program (set may be empty)
        self.kvs = {}

        # list of variables used in program
        self.vars = []

        # Maps variables that are used in expressions involving fields,
        # to the field names. In a sense, it makes a 'type' of a variable.
        # E.g. if you have expression `some_field = $x` then we assume
        # that $x is of `some_field`.
        self.var_fields = {}

        # maps rule id to 'window' rule ids it is contained within
        self.rule_windows = {}

    def get_rule_name(self, ri):
        return self.rules[ri].get('name', str(ri))

    def get_rule_id_by_name(self, name):
        for i, _ in enumerate(self.rules):
            n = self.get_rule_name(i)
            if n == name:
                return i
        raise Exception('Rule not found: %s' % name)

    # return position of window `window_block_name` on the window stack when we
    # are inside rule `ri`
    def get_rule_window_block_stack_pos(self, rule_id, window_block_id):
        for i, wi in enumerate(self.rule_windows[rule_id]):
            if wi == window_block_id:
                return i
        raise Exception('No enclosing window block named %s for rule %s' % (self.get_rule_name(window_block_id), self.get_rule_name(rule_id)))

    def get_rule_window_duration(self, rule_id):
        return self.rules[rule_id].get("window")

    def get_yield_names(self, set_name):
        """
        Get's the names of values yielded to set_name
        Only call this for protobuf programs
        """
        sources = [
            y['src']
            for rule in self.rules
            for clause in rule['clauses']
            if 'yield' in clause
            for y in clause['yield']
            if y['dst'] == set_name
        ]
        names = {tuple(i['name'] for i in src) for src in sources}

        if len(names) != 1:
            raise Exception("Yielding to {}. Protobuf programs must yield the same tuple arities for the same set".format(set_name))

        return names.pop()


def add_yield_vars(program, yield_list):
    for y in yield_list:
        if var_type(y['dst']) == 'scalar':
            program.yield_counters.add(y['dst'])
        elif var_type(y['dst']) == 'set':
            program.yield_sets.add(strip_type(y['dst']))
        elif var_type(y['dst']) == 'multiset':
            program.yield_multisets.add(strip_type(y['dst']))
        elif var_type(y['dst']) == 'hll':
            program.yield_hlls.add(strip_type(y['dst']))
        else:
            assert('bad yield')


def is_special_var(program, varname):
    if varname in ('cookie', 'timestamp'):
        return True
    else:
        return False


def preprocess_yield_term(program, term):
    if term['_k'] == 'field':
        field_name = term['name']
        if not is_special_var(program, field_name) and field_name not in program.kvs:
            program.kvs[field_name] = set()
    elif term['_k'] == 'fcall':
        program.external_functions.append((term['name'], len(term['args'])))
        for a in term['args']:
            preprocess_yield_term(program, a)


def preprocess(program):

    # generate window block info:
    # 1) enumerate window rules in window_rule_ids
    # 2) for every rule, generate a list of rules it is nested into

    window_stack = []
    windows = []
    for i, r in enumerate(program.rules):
        while windows and windows[-1] <= i:
            window_stack.pop()
            windows.pop()
        program.rule_windows[i] = window_stack[:]
        if r.get("outer"):
            program.window_rule_ids.append(i)
            window_stack.append(i)
            windows.append(r.get('outer'))
    while windows and windows[-1] < i:
        window_stack.pop()
        windows.pop()

    # find all yield variables
    for r in program.rules:
        for c in r.get("clauses", []):
            if "yield" in c:
                add_yield_vars(program, c['yield'])
        if "after" in r:
            if "yield" in r["after"]:
                add_yield_vars(program, r['after']['yield'])

    vars = set()
    # find all traildb fields and values used
    for r in program.rules:
        for c in r.get("clauses", []):
            for field, conditions in c["attrs"].items():
                for expr in conditions:
                    expr = expr.lstrip('<=>')
                    if field not in program.kvs and not is_special_var(program, field):
                        program.kvs[field] = set()
                    if not is_variable(expr) and not is_special_var(program, field):
                        program.kvs[field].add(expr)
                    else:
                        if is_variable(expr):
                            vars.add(expr)
                            program.var_fields[expr] = field

            # also find fields that are yielded but never used in conditions
            if c.get('yield'):
                for _yield in c['yield']:
                    if var_type(_yield['dst']) in ('set', 'multiset', 'hll'):
                        for f in _yield.get('src', []):
                            preprocess_yield_term(program, f)

    groupby_vars = program.groupby.get('vars', []) if program.groupby else []
    program.groupby_vars = groupby_vars

    program.vars = list(vars | set(groupby_vars))
    program.no_rewind = is_no_rewind(program)
    program.has_window_rules = len(program.window_rule_ids) > 0

    entrypoint_id = 0
    for i, r in enumerate(program.rules):
        if r.get("entrypoint"):
            entrypoint_id = i
    program.entrypoint_id = entrypoint_id


def is_no_rewind(program):
    # figure out if this state machine ever requires jumping back in the trail
    # makes things a lot easier if it is not
    for r in program.rules:
        for c in r.get("clauses", []):
            action = parse_action(c.get("action", "restart-from-here"))
            if action.type == "restart-from-start":
                return False
        if "after" in r:
            action = parse_action(r['after'].get('action', "restart-from-here"))
            if action.type == "restart-from-start":
                return False
    return True


def compile_block(g, ri, r, program):
    g.o("RULE_START_r%d:" % ri)
    enter_rule(g, ri, program)

    g.o("RULE_CONT_r%d:" % ri)

    if r.get("outer"):
        return

    g.o("state->ri = %d;" % ri)
    g.o('DBG_PRINTF("entering rule \\"%s\\" at pos %%" PRId64 ", timestamp %%" PRIu64 ", end_of_trail %%d\\n", ctx_get_position(ctx), timestamp, ctx_end_of_trail(ctx));' % (program.get_rule_name(ri)))

    if ri == 0 and program.has_window_rules:
        g.o("state->outers[0].id = -1;")
        g.o("state->outers[0].window_expires = 0; ")

    g.o('if (ctx_end_of_trail(ctx)) goto STOP;')
    with BRACES(g, "while (1)"):
        g.o("item = ctx_get_item(ctx);")
        g.o("timestamp = item_get_timestamp(item);")
        g.o("/* check timestamp */")
        g.o("bool within_window = (state->window_expires == 0 || state->window_expires > timestamp);")
        g.o("if (within_window && !item_is_empty(item))")
        with BRACES(g):
            for ci, c in enumerate(r["clauses"]):
                compile_clause(g, ri, ci, c, succ="CONTINUE_r%d" % ri, fail="AFTER_CLAUSE_r%d_c%d" % (ri, ci), program=program)
                g.o("AFTER_CLAUSE_r%d_c%d:" % (ri, ci))
            # no clauses matched
            g.o('error("non-exhaustive clauses at statement %s");' % (program.get_rule_name(ri),))

        with BRACES(g, "if (item_is_empty(item))"):
            g.o("ctx_advance(ctx);")
            g.o('DBG_PRINTF("NOT advancing empty item\\r\\n");')

        if program.has_window_rules:
            with BRACES(g, "for (int i = 0; i < sizeof(state->outers) / sizeof(outer_info_t); i++)"):
                with BRACES(g, "if (state->outers[i].id == -1)"):
                    g.o("break;")
                with BRACES(g, "else"):
                    g.o("bool within_window = (state->outers[i].window_expires == 0 || state->outers[i].window_expires > timestamp);")
                    with BRACES(g, "if (!within_window)"):
                        g.o("int outer_id = state->outers[i].id;")
                        g.o("state->outers[i].id = -1;")
                        g.o("state->outers[i].window_expires = 0;")
                        g.o('DBG_PRINTF("exiting outer %d\\n", outer_id);')
                        with BRACES(g, "switch (outer_id)"):
                            for oi in program.window_rule_ids:
                                g.o("case %d: " % oi)
                                compile_clause_action(g, oi, 'else', program.rules[oi].get("after", {"action" : "restart-from-here"}), program)
                                g.o("break;")
                            g.o('default:error("not supposed to reach this");')

        with BRACES(g):
            compile_clause_action(g, ri, 'else', program.rules[ri].get("after", {"action" : "restart-from-here"}), program)

        g.o("CONTINUE_r%d:" % ri)
        g.o("if (ctx_end_of_trail(ctx)) goto STOP;")


def gen_prologue(g, program, includes):
    g.o("""
        #include <stdint.h>
        #include <stdbool.h>
        #include <string.h>
        #include <stdio.h>
        #include <Judy.h>

        #include "fns_generated.h"
        #include "utils.h"
    """)
    for i in includes:
        g.o("""
        #include "%s"
        """ % i)
    g.o("#if DEBUG")
    g.o("#define DBG_PRINTF(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__);")
    g.o("#else")
    g.o("#define DBG_PRINTF(msg, ...)")
    g.o("#endif")
    g.o("#define MIN(x,y) ((x) < (y) ? (x) : (y))")

    with BRACES(g, "bool set_contains(Pvoid_t set, int value)"):
        g.o("int Rc_int;")
        g.o("J1T(Rc_int, set, value);")
        g.o("return Rc_int == 1;")


def gen_get_param_id(g, program):
    with BRACES(g, "int match_get_param_id(const char *param)"):
        for i, v in enumerate(program.vars):
            with BRACES(g, "if (strcmp(param, \"%s\") == 0)" % v):
                g.o("return %d;" % i)
        g.o("return -1;")


def gen_set_param(g, program):
    with BRACES(g, "int match_set_param(int param_id, int value, kvids_t *ids, char *string_val, int string_val_len)"):
        with BRACES(g, "switch (param_id)"):
            for i, v in enumerate(program.vars):
                if v.startswith('%'):
                    g.o("case %d: ids->var_%s = value;" % (i, v.lstrip('%#')))
                    g.o("ids->varstr_%s = string_val;" % (v.lstrip('%#'), ))
                    g.o("ids->varstrlen_%s = string_val_len;" % (v.lstrip('%#'), ))
                    g.o("break;")
        g.o("return -1;")


def gen_get_param_field(g, program):
    with BRACES(g, "char *match_get_param_field(int param_id)"):
        with BRACES(g, "switch (param_id)"):
            for i, v in enumerate(program.vars):
                # most vars have inferred field; however you can have fields that
                # are never used in conditions (yet yielded), hence they have
                # no "type"
                if v in program.var_fields:
                    g.o("case %d: return \"%s\"; break;" % (i, program.var_fields[v]))
        g.o("return 0;")


def gen_set_list_param(g, program):
    with BRACES(g, "int match_set_list_param(int param_id, Pvoid_t value, kvids_t *ids)"):
        for i, v in enumerate(program.vars):
            if v.startswith('#'):
                with BRACES(g, "switch (param_id)"):
                    g.o("case %d: ids->var_%s = value; break;" % (i, v.lstrip('#')))
        g.o("return -1;")


def gen_free_params(g, program):
    with BRACES(g, "void match_free_params(kvids_t *ids)"):
        g.o("int Rc_word;")
        for v in program.vars:
            if var_type(v) == 'scalar':
                g.o("ids->var_%s = -1;" % strip_type(v))
            elif var_type(v) == 'set':
                g.o("J1FA(Rc_word, ids->var_%s) ;" % strip_type(v))
            else:
                raise Exception('Invalid variable name: %s' % v)


def gen_add_results(g, program):
    with BRACES(g, "static inline void match_add_results(results_t *dst, const results_t *src)"):
        for k in program.yield_counters:
            g.o("dst->%s += src->%s;" % (strip_type(k), strip_type(k)))

        for k in program.yield_sets:
            with BRACES(g):
                g.o("set_add(&dst->set_%s, &src->set_%s);" % (k, k))
        for k in program.yield_multisets:
            with BRACES(g):
                g.o("mset_add(&dst->mset_%s, &src->mset_%s);" % (k, k))
        for k in program.yield_hlls:
            with BRACES(g):
                g.o("dst->hll_%s = hll_merge(dst->hll_%s, src->hll_%s);" % (k, k, k))


def gen_free_results(g, program):
    with BRACES(g, "static inline void match_free_results(results_t *dst)"):
        for k in program.yield_sets:
            g.o("set_free(&dst->set_%s);" % (k,))
        for k in program.yield_multisets:
            g.o("set_free(&dst->mset_%s);" % (k,))
        for k in program.yield_hlls:
            g.o("hll_free(dst->hll_%s);" %(k,))


def gen_is_zero_result(g, program):
    with BRACES(g, "static inline bool match_is_zero_result(results_t *r)"):
        g.o("return true")
        for k in program.yield_counters:
            g.o("&& (r->%s == 0)" % (strip_type(k),))
        for k in program.yield_sets:
            g.o("&& (r->set_%s == NULL)" % (k,))
        for k in program.yield_multisets:
            g.o("&& (r->mset_%s == NULL)" % (k,))
        for k in program.yield_hlls:
            g.o("&& (r->hll_%s == NULL)" % (k,))

        g.o(";")


def gen_structs(g, program):
    g.o("#pragma pack (push, 1)")
    g.o("""
        typedef struct {
            timestamp_t window_expires;
            int id;
        } outer_info_t;

        """)
    with BRACES(g, "struct results_t"):
        for k in program.yield_counters:
            g.o("uint64_t %s;" % k.lstrip("$"))
        for k in program.yield_sets:
            g.o("set_t set_%s;" % k)
        for k in program.yield_multisets:
            g.o("set_t mset_%s;" % k)
        for k in program.yield_hlls:
            g.o("hyperloglog_t *hll_%s;" %k)
    g.o(";")
    g.o("")

    with BRACES(g, "struct kvids_t"):
        for k in program.kvs:
            if k != 'timestamp':
                g.o("int key_%s;" % k)
        for k in program.kvs:
            if k != 'timestamp':
                for v in program.kvs[k]:
                    g.o("int value_%s_%s;" % (k, escape_var_name(v)))

        for v in program.vars:
            if var_type(v) == 'scalar':
                g.o("int var_%s;" % strip_type(v))
                g.o("char *varstr_%s;" % strip_type(v))
                g.o("int varstrlen_%s;" % strip_type(v))
            elif var_type(v) == 'set':
                g.o("Pvoid_t var_%s;" % strip_type(v))
            else:
                assert(not v)

    g.o(";")
    g.o("")

    with BRACES(g, "struct state_t"):
        if not program.no_rewind:
            g.o("int start;")

        g.o("int ri;")
        g.o("timestamp_t window_expires;")
        if program.has_window_rules:
            g.o("outer_info_t outers[%d];" % (len(program.window_rule_ids) + 1))
    g.o(";")
    g.o("")
    g.o("#pragma pack (pop)")


def gen_print(g, program):
    with BRACES(g, "void match_save_result(results_t *results, void *arg, void (*save_int)(void *, char *, int64_t), void (*save_set)(void *, char *, set_t *), void (*save_multiset)(void *, char *, set_t *), void (*save_hll)(void *, char *, hyperloglog_t *))"):
        for i, k in enumerate(program.yield_counters):
            g.o("save_int(arg, \"%s\", results->%s);" % (k, strip_type(k)))
        for i, k in enumerate(program.yield_sets):
            g.o("save_set(arg, \"#%s\", &results->set_%s);" % (k, k))
        for i, k in enumerate(program.yield_multisets):
            g.o("save_multiset(arg, \"&%s\", &results->mset_%s);" % (k, k))
        for i, k in enumerate(program.yield_hlls):
            g.o("save_hll(arg, \"^%s\", results->hll_%s);" % (k, k))


def gen_db_init(g, program):
    with BRACES(g, "void match_db_init(kvids_t *ids, db_t *db)"):
        g.o('DBG_PRINTF("========== match_db_init() ===========\\n")')
        for k in program.kvs:
            if k != 'timestamp':
                g.o("ids->key_%s = db_get_key_id(\"%s\", db);" % (k, k))
                g.o("""DBG_PRINTF("ids->key_{k} = %d\\n", ids->key_{k});""".format(k = k))
        for k in program.kvs:
            if k != 'timestamp':
                for v in program.kvs[k]:
                    g.o("ids->value_%s_%s = db_get_value_id(\"%s\", %d, ids->key_%s, db);" % (k, escape_var_name(v), v, len(v), k))
                    g.o("""DBG_PRINTF("ids->value_{k}_{v} = %d\\n", ids->value_{k}_{v});""".format(k = k, v = escape_var_name(v)))

        for v in program.vars:
            if var_type(v) == 'set':
                g.o("ids->var_%s = NULL;" % strip_type(v))
            elif var_type(v) == 'scalar':
                g.o("ids->var_%s = -1;" % strip_type(v))
                g.o("ids->varstr_%s = 0;" % strip_type(v))
                g.o("ids->varstrlen_%s = 0;" % strip_type(v))
            else:
                raise Exception('bad variable name %s' % v)

        g.o('DBG_PRINTF("==========================\\n")')


def gen_trail_init(g, program):
    with BRACES(g, "void match_trail_init(state_t *state)"):
        g.o("state->window_expires = %s;" % EXPIRES_NEVER)
        if not program.no_rewind:
            g.o("state->start = 0;")
        g.o("state->ri = %d;" % program.entrypoint_id)
        if program.has_window_rules:
            g.o("state->outers[0].id = -1;")
            g.o("state->outers[0].window_expires = 0;")


def gen_is_initial_state(g, program):
    with BRACES(g, "bool match_is_initial_state(state_t *state)"):
        g.o("if (state->window_expires != 0 && state->window_expires != %s) return false;" % EXPIRES_NEVER)
        if not program.no_rewind:
            g.o("if (state->start != 0) return false;")
        g.o("if (state->ri != %d) return false;" % program.entrypoint_id)
        if program.has_window_rules:
            g.o("if (state->outers[0].id != -1) return false;")
        g.o("return true;")


def gen_match_same_state(g, program):
    with BRACES(g, "bool match_same_state(state_t *a, state_t *b)"):
        g.o("if (a->ri != b->ri) return false;")
        g.o("if (a->window_expires != b->window_expires) return false;")
        if not program.no_rewind:
            g.o("if (a->start != b->start) return false;")

        if program.has_window_rules:
            for i in range(len(program.window_rule_ids)):
                g.o("if (a->outers[%d].id != b->outers[%d].id) return false;" % (i, i))
                g.o("if (a->outers[%d].id == -1) return true;" % i)
                g.o("if (a->outers[%d].window_expires != b->outers[%d].window_expires) return false;" % (i, i))
        g.o("return true;")


def gen_get_result_size(g, program):
    with BRACES(g, "size_t match_get_result_size()"):
        g.o("return sizeof(results_t);")


def make_ast(rules, groupby):
    program = Program(rules, groupby=groupby)
    preprocess(program)
    return program


def compile(program, includes, out=sys.stdout):
    g = Gen(out)
    gen_prologue(g, program, includes=includes)
    gen_db_init(g, program)
    gen_trail_init(g, program)
    gen_is_initial_state(g, program)
    gen_set_param(g, program)
    gen_set_list_param(g, program)
    gen_get_param_id(g, program)
    gen_get_param_field(g, program)
    gen_free_params(g, program)
    gen_print(g, program)
    gen_match_same_state(g, program)
    gen_get_result_size(g, program)
    gen_external_function_declarations(g, program)
    g.o("int match_trail(state_t *state, results_t *results, kvids_t *ids, ctx_t *ctx)")
    with BRACES(g):
        # memcpy just for convenience
        g.o("int abort = 0;")
        g.o("timestamp_t timestamp = 0;")
        g.o("item_t item = 0;")
        g.o("switch (state->ri)")
        with BRACES(g):
            for i, r in enumerate(program.rules):
                g.o("case %d: goto RULE_CONT_r%d;" % (i, i))
            g.o("case -1: abort = 1; goto STOP; ")
        for i, r in enumerate(program.rules):
            compile_block(g, i, r, program)
        g.o("STOP:")
        g.o('DBG_PRINTF("================== STOP =================\\n");')
        g.o("return abort;")


def compile_proto(program, includes, proto_info, out=sys.stdout):
    g = Gen(out)
    gen_prologue_proto(g, program, proto_info, includes=includes)
    gen_proto_add_int(g, program, proto_info)
    gen_proto_add_set(g, program, proto_info)
    gen_proto_add_multiset(g, program, proto_info)
    gen_proto_add_hll(g, program, proto_info)
    gen_output_groupby_result_proto(g, program, proto_info)
    gen_output_proto(g, program, proto_info)


def gen_external_function_declarations(g, program):
    for fname, nargs in set(program.external_functions):
        args = ["char *", "int"]
        for narg in xrange(nargs):
            args.append("char *")
            args.append("int")

        g.o("int %s(%s);" % (fname, ",".join(args)))


def gen_header(program, groupby, out=sys.stdout):
    g = Gen(out)
    g.o("#ifndef __OUT_TRAILDB_H__")
    g.o("#define __OUT_TRAILDB_H__")
    g.o("#define EXPIRES_NEVER %s" % EXPIRES_NEVER)
    g.o("#include <json-c/json.h>")
    g.o('#include "utils.h"')
    gen_structs(g, program)

    g.o("static inline bool match_no_rewind() { return %s; }" % ('true' if program.no_rewind else 'false'))
    merge_results = groupby.get('merge_results', False) if groupby else False
    g.o("static int match_num_groupby_vars = %d;" % len(program.groupby_vars))
    g.o("static int match_merge_results = %d;" % (1 if merge_results else 0))
    g.o("static char *match_groupby_vars[] = {%s};" % ','.join(('"%s"' % v) for v in program.groupby_vars))
    g.o("static char *match_groupby_array_param = %s;" % ('"%s"' % groupby.get('values') if groupby and 'values' in groupby else 'NULL'))

    free_vars = set(program.vars) - set(program.groupby_vars)
    g.o("static int match_num_free_vars = %d;" % len(free_vars))
    g.o("static char *match_free_vars[] = {%s};" % ','.join(('"%s"' % v) for v in free_vars))

    gen_add_results(g, program)
    gen_free_results(g, program)
    gen_is_zero_result(g, program)
    g.o("#endif")


def gen_prologue_proto(g, program, proto_info, includes):
    g.o("""
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <Judy.h>
#include <traildb.h>
#include "fns_generated.h"
#include "foreach_util.h"
#include "utils.h"
#include "safeio.h"
#include "hyperloglog.h"
#include "results_protobuf.h"
""")

    for i in includes:
        g.o("#include \"{}\"".format(i))

    g.o("#if DEBUG")
    g.o("#define DBG_PRINTF(msg, ...) fprintf(stderr, msg, ##__VA_ARGS__);")
    g.o("#else")
    g.o("#define DBG_PRINTF(msg, ...)")
    g.o("#endif")
    g.o("#define MAXLINELEN 1000000")

    g.o("const static Trck__SetTuple TRCK_SET_TUPLE_DEFAULT = TRCK__SET_TUPLE__INIT;")
    g.o("const static Trck__MultisetTuple TRCK_MULTISET_TUPLE_DEFAULT = TRCK__MULTISET_TUPLE__INIT;")
    g.o("const static Trck__Hll TRCK_HLL_DEFAULT = TRCK__HLL__INIT;")

    g.o("const int protobuf_enabled = 1;")


def gen_proto_add_int(g, program, proto_info):
    with BRACES(g, "void proto_add_int(void *p, char *name, int64_t value)"):
        g.o("{struct} *msg = ({struct} *) p;".format(struct=proto_info.to_struct()))
        for yield_counter in program.yield_counters:
            counter = ph.proto_counter(yield_counter)
            with BRACES(g, "if (!strcmp(name, \"{}\"))".format(yield_counter)):
                g.o("msg->{} = value;".format(counter))


def gen_proto_add_set(g, program, proto_info):
    with BRACES(g, "void proto_add_set(void *p, char *name, set_t *value)"):
        g.o("{struct} *msg = ({struct} *) p;".format(struct=proto_info.to_struct()))
        for yield_set in program.yield_sets:
            set_name = ph.proto_set(yield_set)
            with BRACES(g, "if (!strcmp(name, \"#{}\"))".format(yield_set), set=set_name):
                g.co("msg->n_{set} = JSL_size(value);")
                g.co("msg->{set} = malloc(msg->n_{set} * sizeof(void *));")

                g.o("int i = 0;")
                g.o("uint8_t index[MAXLINELEN];")
                g.o("index[0] = '\\0';")
                g.o("Word_t *pv;")
                g.o("JSLF(pv, *value, index);")
                with BRACES(g, "while(pv)"):
                    g.o("char buf[1024];")

                    g.o("char *tail = (char*) index;")
                    g.o("int res_len;")
                    g.o("int res_type;")
                    g.co("msg->{set}[i] = malloc(sizeof(Trck__SetTuple));")
                    g.co("*(msg->{set}[i]) = TRCK_SET_TUPLE_DEFAULT;")
                    g.o("int size = string_tuple_size(tail);")
                    g.co("msg->{set}[i]->values = malloc(size * sizeof(char *));")
                    g.co("msg->{set}[i]->n_values = size;")
                    g.o("int j = 0;")
                    with BRACES(g, "while(!string_tuple_is_empty(tail))"):
                        g.o("tail = string_tuple_extract_head(tail, sizeof(buf), (uint8_t *)buf, &res_len, &res_type);")
                        g.o("buf[res_len] = '\\0';")
                        g.co("msg->{set}[i]->values[j] = malloc(sizeof(char) * (res_len + 1));")
                        g.co("strncpy(msg->{set}[i]->values[j], buf, res_len + 1);")
                        g.o("j++;")

                    g.o("i++;")
                    g.o("JSLN(pv, *value, index);")


def gen_proto_add_multiset(g, program, proto_info):
    with BRACES(g, "void proto_add_multiset(void *p, char *name, set_t *value)"):
        g.o("{struct} *msg = ({struct} *) p;".format(struct=proto_info.to_struct()))
        for yield_multiset in program.yield_multisets:
            set_name = ph.proto_multiset(yield_multiset)
            with BRACES(g, "if (!strcmp(name, \"&{}\"))".format(yield_multiset), set=set_name):
                g.co("msg->n_{set} = JSL_size(value);")
                g.co("msg->{set} = malloc(msg->n_{set} * sizeof(void *));")

                g.o("int i = 0;")
                g.o("uint8_t index[MAXLINELEN];")
                g.o("index[0] = '\\0';")
                g.o("Word_t *pv;")
                g.o("JSLF(pv, *value, index);")
                with BRACES(g, "while(pv)"):
                    g.o("char buf[1024];")
                    g.o("char *tail = (char*) index;")
                    g.o("int res_len;")
                    g.o("int res_type;")

                    g.co("msg->{set}[i] = malloc(sizeof(Trck__MultisetTuple));")
                    g.co("*(msg->{set}[i]) = TRCK_MULTISET_TUPLE_DEFAULT;")
                    g.o("int size = string_tuple_size(tail);")
                    g.co("msg->{set}[i]->values = malloc(size * sizeof(char *));")
                    g.co("msg->{set}[i]->n_values = size;")
                    g.co("msg->{set}[i]->count = *pv;")
                    g.o("int j = 0;")
                    with BRACES(g, "while(!string_tuple_is_empty(tail))"):
                        g.o("tail = string_tuple_extract_head(tail, sizeof(buf), (uint8_t *)buf, &res_len, &res_type);")
                        g.o("buf[res_len] = '\\0';")
                        g.co("msg->{set}[i]->values[j] = malloc(sizeof(char) * (res_len + 1));")
                        g.co("strncpy(msg->{set}[i]->values[j], buf, res_len + 1);")
                        g.o("j++;")

                    g.o("i++;")
                    g.o("JSLN(pv, *value, index);")


def gen_proto_add_hll(g, program, proto_info):
    with BRACES(g, "void proto_add_hll(void *p, char *name, hyperloglog_t *value)"):
        g.o("{struct} *msg = ({struct} *) p;".format(struct=proto_info.to_struct()))
        for yield_hll in program.yield_hlls:
            hll = ph.proto_hll(yield_hll)
            with BRACES(g, "if (!strcmp(name, \"^{}\"))".format(yield_hll), hll=hll):
                g.co("msg->{hll} = malloc(sizeof(Trck__Hll));")
                g.co("*msg->{hll} = TRCK_HLL_DEFAULT;")
                with BRACES(g, "if (value)"):
                    g.co("msg->{hll}->precision = value->p;")
                    g.co("msg->{hll}->empty = 0;")
                    g.o("const char * encodedHll = hll_to_string(value);")
                    g.co("msg->{hll}->bins.data = (uint8_t*) encodedHll + 4;")
                    g.co("msg->{hll}->bins.len = strlen(encodedHll) - 4;")
                with BRACES(g, "else"):
                    g.co("msg->{hll}->precision = 14;")
                    g.co("msg->{hll}->empty = 1;")
                    g.co("msg->{hll}->bins.data = 0;")
                    g.co("msg->{hll}->bins.len = 0;")


def gen_output_groupby_result_proto(g, program, proto_info):
    with BRACES(g, "void output_groupby_result_proto(groupby_info_t *gi, int i, results_t *results)"):
        g.o("{struct} msg = {struct_init};".format(
            struct=proto_info.to_struct(),
            struct_init=proto_info.to_struct_init()))
        g.o("string_val_t *tuple = &gi->tuples[i * gi->num_vars];")
        g.o("results_t *pres = (results_t *)((uint8_t *)results + match_get_result_size() * i);")

        for index, param in enumerate(program.groupby['vars']):
            param_name = ph.proto_scalar(param)
            g.push_context("param_name", param_name)
            g.push_context("index", index)
            g.co("msg.{param_name} = malloc(sizeof(char) * (tuple[{index}].len + 1));")
            g.co("strncpy(msg.{param_name}, tuple[{index}].str, tuple[{index}].len);")
            g.co("msg.{param_name}[tuple[{index}].len] = '\\0';")

        g.o("match_save_result(" \
            "pres, " \
            "&msg, " \
            "proto_add_int, " \
            "proto_add_set, " \
            "proto_add_multiset, " \
            "proto_add_hll);")

        g.o("unsigned long len = {}(&msg);".format(proto_info.to_get_packed_size()))
        g.o("void *buf = malloc(len);")
        g.o("{}(&msg, buf);".format(proto_info.to_pack()))

        g.o("fwrite(&len, sizeof(unsigned long), 1, stdout);")
        g.o("fwrite(buf, len, 1, stdout);")
        g.o("free(buf);")


def gen_output_proto(g, program, proto_info):
    with BRACES(g, "void output_proto(groupby_info_t *gi, results_t *results)"):
        with BRACES(g, "for (int i = 0; i < gi->num_tuples; i++)"):
            g.o("output_groupby_result_proto(gi, i, results);")


if __name__ == '__main__':
    if sys.argv[1] == "matcher":
        js = json.load(sys.stdin)
        compile(js["rules"], includes=sys.argv[2:], groupby=js.get('groupby'))
    elif sys.argv[1] == 'header':
        js = json.load(sys.stdin)
        gen_header(js["rules"], groupby=js.get('groupby'))
