
import sys
import tempfile
import json

from collections import defaultdict, OrderedDict
from subprocess import Popen, PIPE
from itertools import product, count, izip, chain, cycle

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
MONTH = 30 * DAY

MAX_WINDOW = MONTH + 2 * DAY

#
# States
#

class State(object):
    def emit_event(self, t, inputs):
        return []

    def emit_negative_events(self, start_t, end_t, inputs, bound, num_blocks=4):
        return []

    def increment_time(self, t):
        return t

class EventState(State):
    def __init__(self, fields, must_change):
        self.fields = fields
        self.must_change = frozenset(must_change)
        self.must_change_iter = cycle(self.must_change)
        self.input_fields = [e for e in self.fields.iteritems()
                             if not isinstance(e[1], list) and
                                (e[1][0] == '%' or e[1][0] == '#')]
        self.negate_count = 0

    def emit_event(self, t, inputs):
        f = self.fields.copy()
        f['timestamp'] = t
        for k, v in self.input_fields:
            f[k] = inputs[v]
            if isinstance(f[k], tuple):
                f[k] = f[k][0]
        yield f

    def emit_negative_events(self, start_t, end_t, inputs, bound, num_blocks=4):
        step = (end_t - start_t) / num_blocks
        for i in range(num_blocks):
            t = start_t + i * step
            for field in self.fields:
                f = self.emit_event(t, inputs).next()
                f[field] = self.negate(field, f[field], bound)
                if self.must_change and field not in self.must_change:
                    must = self.must_change_iter.next()
                    f[must] = self.negate(must, f[must], bound)
                yield f

    def negate(self, field, value, bound_values):
        self.negate_count += 1
        if field in bound_values:
            allowed = bound_values[field] - {value}
            if allowed:
                # make the selection deterministic by sorting values
                return sorted(allowed)[self.negate_count % len(allowed)]
        return value + '_FOO'

    def __repr__(self):
        return '[%s]' % ' '.join('%s=%s' % x for x in self.fields.iteritems())

class TimeState(State):
    def __init__(self, delta, window):
        self.delta = delta
        self.window = window

    def increment_time(self, t):
        return t + self.delta

#
# Constraints
#

class Constraint(object):
    def bound_values(self):
        return []

    def is_event(self):
        return 0

class EventConstraint(Constraint):
    def __init__(self, bound_fields={}, must_change=[]):
        self.bound_fields = bound_fields
        self.set_values = []
        self.set_keys = []
        self.must_change = must_change
        for k, v in self.bound_fields.iteritems():
            if isinstance(v, list):
                self.set_values.append(v)
                self.set_keys.append(k)

    def __iter__(self):
        if self.set_keys:
            for set_values in product(*self.set_values):
                f = self.bound_fields.copy()
                f.update(izip(self.set_keys, set_values))
                yield EventState(f, self.must_change)
        else:
            yield EventState(self.bound_fields.copy(), self.must_change)

    def bound_values(self):
        for k, v in self.bound_fields.iteritems():
            if k not in self.set_keys and v[0] not in ('%', '#'):
                yield k, v

    def is_event(self):
        return 1

class TimeConstraint(Constraint):
    def __init__(self, min=0, max=MAX_WINDOW, steps=4):
        self.min = min
        self.max = max
        self.step = ((self.max - 1) - (self.min + 1)) / steps

    def __iter__(self):
        for sec in range(self.min, self.max - self.min, self.step):
            yield TimeState(sec, self.max)

#
# Generate trails
#

def get_bound_values(constraints):
    bound_values = defaultdict(set)
    for k, v in chain.from_iterable(c.bound_values() for c in constraints):
       bound_values[k].add(v)
    return bound_values

def foreach_input(input_keys):
    for i in count():
        d = OrderedDict()
        for x in input_keys:
            v = '%s%d' % (x[1:], i)
            if x[0] == '#':
                v = (v,)
            d[x] = v
        yield d

def generate_trails(constraints, input_iter, stay_positive=True, num_neg=10):
    bound_values = get_bound_values(constraints)

    if stay_positive:
        negative_states = range(1)
    else:
        negative_states = range(1, 2**sum(c.is_event() for c in constraints))

    res = {}
    for negativity_mask in negative_states:
        for constr in product(*constraints):
            t = prev_t = 0
            seq = []
            inputs = input_iter.next()
            e = 0
            for j, state in enumerate(constr):

                # 1) Generate negative events between prev_t - t
                seq.extend(state.emit_negative_events(prev_t,
                                                      t,
                                                      inputs,
                                                      bound_values))

                # 2) Increment time
                prev_t = t
                t = state.increment_time(t)

                # 3) Generate a positive event, unless in negative mode
                if constraints[j].is_event():
                    if (1 << e) & negativity_mask:
                        seq.extend(state.emit_negative_events(t,
                                                              t,
                                                              inputs,
                                                              bound_values,
                                                              1))
                    else:
                        seq.extend(state.emit_event(t, inputs))
                    e += 1

            res[tuple(inputs.items())] = seq
    return res

#
# Parser
#

def parse_trick_spec(trick_spec, param_set_size=10):

    def ssplit(x, delim=','):
        return tuple(t.strip() for t in x.split(delim))

    def parse_time(timespec):
        value, unit = timespec.strip().split()
        f = {'days': DAY, 'hours': HOUR, 'minutes': MINUTE, 'seconds': 1}
        return int(value) * f[unit]

    def parse_param(param):
        if param[0] == '#':
            if '=' in param:
                return k, v.split()
            else:
                return param, [param[1:] + str(i) for i in range(param_set_size)]
        elif param[0] == '%':
            return ssplit(param, '=')
        else:
            raise Exception("Unknown parameter: %s" % param)

    def parse_field(field, params, must_change):
        must = False
        if field[0] == '!':
            field = field[1:]
            must = True
        if '=' in field:
            k, v = ssplit(field, '=')
        else:
            k, v = ssplit(field, ' in ')
            v = params.get(v, v)
        if must:
            must_change.add(k)
        return k, v

    def parse_output(output):
        k, v = ssplit(x, '=')
        return k, int(v)

    case = {}
    for i, line in enumerate(open(trick_spec)):
        line = line.strip()

        if not line:
            pass

        elif line.startswith('#'):
            if case:
                yield case
            case = {'title': line[1:].strip(), 'constraints': []}

        elif line.startswith('Window'):
            case['window'] = parse_time(line.split(':')[1])

        elif line.startswith('Input'):
            key, vals = line.split(':')[1].split('=')
            case['input'] = (key.strip(), ssplit(vals))

        elif line.startswith('Output') or line.startswith('-Output'):
            key = line.split(':')[0].strip().lower()
            case[key] = [parse_output(x) for x in line.split(':')[1].split(',')]

        elif line.startswith('Params') or line.startswith('-Params'):
            key = line.split(':')[0].strip().lower()
            case[key] = dict(parse_param(x) for x in ssplit(line.split(':')[1]))

        elif line.startswith('Positive'):
            case['only_positive'] = True

        elif line[0] == '[' and line[-1] == ']':
            cons = case['constraints']
            params = case['params']
            must_change = set()
            e = dict(parse_field(x, params, must_change)
                     for x in ssplit(line[1:-1]))
            if cons and isinstance(cons[-1], EventConstraint):
                cons.append(TimeConstraint(0, case.get('window', MAX_WINDOW)))
            cons.append(EventConstraint(e, must_change))

        elif line[0] == '<' and line[-1] == '>':
            args = {'min': 1, 'max': case.get('window', MAX_WINDOW)}
            for arg in line[1:-1].split(','):
                minmax, timespec = arg.strip().split(' ', 1)
                args[minmax] = parse_time(timespec)
            case['constraints'].append(TimeConstraint(**args))

        else:
            raise Exception("Cannot parse line %d in %s: %s" %\
                            (i + 1, trick_spec, line))
    yield case

def generate_tests(trick_spec):
    for test_case in parse_trick_spec(trick_spec):
        input_name, input_keys = test_case['input']

        cases = [(test_case['title'],
                  test_case['output'],
                  test_case['params'],
                  True)]

        if '-params' in test_case:
            cases.append(('%s (alternative case)' % test_case['title'],
                          test_case['-output'],
                          test_case['-params'],
                          True))

        if 'only_positive' not in test_case:
            cases.append(('%s (negative case)' % test_case['title'],
                          [(k, 0) for k, v in test_case['output']],
                          test_case['params'],
                          False))

        for title, output, params, stay_positive in cases:

            trails = generate_trails(test_case['constraints'],
                                     foreach_input(input_keys),
                                     stay_positive)
            tests = format_tests(trails, output, input_name, params)
            yield title, tests, trails, input_keys

def format_tests(trails, expect, inputs_name, params):
    cookie_trails = dict(('cookie' + str(i), t)
                         for i, t in enumerate(trails.itervalues()))
    expected = [dict(chain(k, expect)) for k in trails]
    params[inputs_name] = [[v for _, v in item] for item in trails]
    return {'tests': [{'trails': [cookie_trails], 'expected': expected}],
            'params': params}

#
# Execute
#

def write_test_file(tr_file, tests):
    out = tempfile.NamedTemporaryFile(prefix='trick-',
                                      suffix='.tr',
                                      dir='/tmp',
                                      delete=False)
    tr_body = open(tr_file).read()
    txt = "%s\n\n----- unit tests ----\n-- %s\n" % (tr_body, json.dumps(tests))
    out.write(txt)
    out.close()
    return out.name

def parse_stderr(stderr, trails, input_keys, test_file):
    def tuplify(x):
        return tuple(x) if isinstance(x, list) else x

    n = 0
    with open('%s.fail' % test_file, 'w') as out:
        for line in stderr.splitlines():
            if line.startswith('expected '):
                n += 1
                exp = json.loads(line.split(' got ')[1])
                key = tuple((k, tuplify(exp[k])) for k in input_keys)
                out.write('%s\ntrail: %s\n---\n' %
                          (line, json.dumps(trails[key])))
    return n

def run(tr_file, trick_spec):
    for title, tests, trails, input_keys in generate_tests(trick_spec):
        test_file = write_test_file(tr_file, tests)
        print "Test: %s @ %s" % (title, test_file)
        proc = Popen(['./run_test.sh', test_file], stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            n = parse_stderr(stderr, trails, input_keys, test_file)
            print "-> %d / %d trails FAILED" % (n, len(trails))
            print "-> Failed cases written to %s.fail" % test_file
        else:
            print "-> %d / %d trails ok!" % (len(trails), len(trails))

if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])


