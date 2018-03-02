from __future__ import print_function

import ply.lex as lex
from itertools import groupby
from datetime import datetime
import re

class ParseError(Exception):
    def __init__(self, message, **kwargs):
        super(Exception, self).__init__(message)
        self.info = kwargs

reserved = set(['after', 'receive', 'yield', 'quit', 'window', 'repeat', 'in', 'foreach', 'to', 'merged', 'results',
                'start_timestamp'])

tokens = [
    'TIMEDELTA',
    'TIMESTAMP', 'STRING', 'NUMBER',
    'COMMA',
    'WILDCARD', 'ARROW', 'EQ', 'LT', 'GT', 'LTE', 'GTE',
    'SCALAR', 'HASH', 'SCALAR_RESULT', 'ARRAY', 'MULTISET', 'HLL',
    'ID', 'WS', 'INDENT', 'NEWLINE', 'DEDENT', 'LBRACKET', 'RBRACKET',
    'LPAREN', 'RPAREN'
    ] + [r.upper() for r in reserved]

type_names = {
    'ID': 'identifier'
}
# Tokens

t_LT      = r'<'
t_GT      = r'>'
t_LTE     = r'<='
t_GTE     = r'>='
t_COMMA   = r','
t_WILDCARD= r'\*'
t_ARROW   = r'->'
t_EQ      = r'='
t_LBRACKET = '\['
t_RBRACKET = '\]'
t_LPAREN = '\('
t_RPAREN  = '\)'
#t_WS = r'[ \t]+'

def t_TIMEDELTA(t):
    r'\d+(s|m|h|d)'
    try:
        t.value = int(t.value[:-1]), t.value[-1]
    except ValueError:
        print("Integer value too large %d", t.value)
        t.value = 0
    return t

def t_NUMBER(t):
    r'\d+'
    try:
        t.value = int(t.value)
    except ValueError:
        print("Integer value too large %d", t.value)
        t.value = 0
    return t

def t_TIMESTAMP(t):
    r'\'\d{4}-\d{2}-\d{2}\''
    try:
        t.value = int((datetime.strptime(t.value.strip("'"), '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds())
    except ValueError:
        print("Cannot parse datetime", t.value)
        t.value = 0
    return t

def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*'
    t.type = t.value.upper() if t.value in reserved else 'ID'
    return t

def t_SCALAR(t):
    r'%[a-zA-Z_][a-zA-Z_0-9]*'
    return t

def t_HASH(t):
    r'\#[a-zA-Z_][a-zA-Z_0-9]*'
    return t

def t_MULTISET(t):
    r'&[a-zA-Z_][a-zA-Z_0-9]*'
    return t

def t_HLL(t):
    r'\^[a-zA-Z_][a-zA-Z_0-9]*'
    return t

def t_ARRAY(t):
    r'@[a-zA-Z_][a-zA-Z_0-9]*'
    return t

def t_SCALAR_RESULT(t):
    r'\$[a-zA-Z_][a-zA-Z_0-9]*'
    return t

def t_STRING(t):
    r'("(\\"|[^"])*")|(\'(\\\'|[^\'])*\')'
    t.value = t.value[1:-1]
    return t

def t_comment(t):
    r"[ ]*--[^\n]*"
    pass

def t_ws(t):
    r'[ ]+'
    t.type = 'WS'
    return t

def t_newline_escape(t):
    r"\\\n"
    pass

def t_newline(t):
    r'\n'
    t.lexer.lineno += t.value.count("\n")
    t.type = 'NEWLINE'
    t.value = ''
    return t

#def t_indent(t):
#    r'\n+[ \t]*'
#    t.lexer.lineno += t.value.count("\n")
#    t.type = 'INDENT'
#    return t

def t_error(t):
    if t.lineno == -1:
        raise ParseError(message="Lexer error: unexpected EOF")
    else:
        raise ParseError(message="Lexer error at line %s position %s: invalid token %s" % (t.lineno, t.lexpos, t.value),
                         lineno=t.lineno,
                         lexpos=t.lexpos,
                         type=t.type,
                         value=t.value)


class IndentLexer:
    def __init__(self, lexer):
        self.lexer = lexer
        self.gen = gen_dedents(gen_indents(skip_begin_newlines(lexer)))

    def input(self, *args, **kwds):
        self.lexer.input(*args, **kwds)

    def token(self):
        try:
            return self.gen.next()
        except StopIteration:
            return None

    def __iter__(self):
        return gen_dedents(gen_indents(skip_begin_newlines(self.lexer)))


def indent_level(v):
    spaces = v.replace("\t", "    ").replace("\n", "")
    return len(spaces)

def skip_begin_newlines(lexer):
    program_started = False
    for token in lexer:
        if program_started:
            yield token
        else:
            if token.type not in ('NEWLINE', 'WS'):
                program_started = True
                yield token

def gen_indents(lexer):
    prev = None
    line_started = False
    for token in lexer:
        #print token
        if token.type not in ('NEWLINE', 'WS'):
            if not line_started:
                line_started = True
                if prev :
                    yield _new_token('INDENT', token.lineno, value=prev.value)
            yield token
            prev = token
        elif token.type == 'NEWLINE':
            line_started = False
            prev = token
        elif token.type == 'WS':
            prev = token

def gen_dedents(lexer):
    stack = [0]
    for token in lexer:

        if token.type != 'INDENT':
            yield token
        else:
            level = indent_level(token.value)
            if level == stack[-1]:
                yield _new_token('NEWLINE', token.lineno)
                continue
            elif level < stack[-1]:
                while stack[-1] > level:
                    stack_level = stack.pop()
                    if stack_level > 0:
                        yield _new_token('DEDENT', token.lineno)
                if stack[-1] != level:
                    raise ParseError("Indent level doesn't match earlier at %s: stack %s now %s" % (token.lineno, stack, level))
            elif level > stack[-1]:
                stack.append(level)
                yield _new_token('INDENT', token.lineno)
    while stack:
        stack_level = stack.pop()
        if stack_level > 0:
            yield _new_token('DEDENT', -1)



def _new_token(type, lineno, value=None):
    tok = lex.LexToken()
    tok.type = type
    tok.lineno = lineno
    tok.value = value
    tok.lexpos = -100
    return tok

def timedelta_to_seconds(n, unit):
    if unit == 's':
        return n
    elif unit == 'm':
        return n * 60
    elif unit == 'h':
        return n * 60 * 60
    elif unit == 'd':
        return n * 60 * 60 * 24
    else:
        raise ParseError("unknown time unit: %s" % unit)

def p_program(p):
    """program : foreach_expr INDENT rules DEDENT
                 | rules"""
    if len(p) > 2:
        p[0] = {'rules' : p[3], 'groupby' : p[1]}
    else:
        p[0] = {'rules' : p[1]}

def p_foreach_expr(p):
    """ foreach_expr : FOREACH vars IN ARRAY
                    |  FOREACH vars IN ARRAY MERGED
                    |  FOREACH vars IN ARRAY MERGED RESULTS """
    p[0] = {'vars': p[2], 'values': p[4], "lineno": p.lineno(2)}
    if len(p) > 5:
        p[0]['merge_results'] = True

def p_foreach_expr_imp(p):
    """ foreach_expr : FOREACH SCALAR
                     | FOREACH SCALAR MERGED
                     | FOREACH SCALAR MERGED RESULTS """
    p[0] = {'vars': [p[2]], "lineno": p.lineno(2)}
    if len(p) > 3:
        p[0]['merge_results'] = True

def p_vars(p):
    """vars : vars COMMA var
             | var """
    if len(p) > 2:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]

def p_var(p):
    """ var : HASH
            | SCALAR
    """
    p[0] = p[1]

def p_rules(p):
    """rules : rules rule
             |  rule """
    if len(p) > 2:
        p[0] = p[1] + [p[2]]
    else:
        p[0] = [p[1]]

def p_rule(p):
    """ rule : ID ARROW INDENT rule_body DEDENT
    """
    p[0] = {k : v for k, v in p[4].items() + [('name', p[1])]}

def p_rule_body(p):
    """ rule_body : window_stmt
                  | receive_stmt
    """
    p[0] = p[1]

def p_windowed_rule(p):
    """ window_stmt : WINDOW INDENT rules DEDENT AFTER TIMEDELTA ARROW actions
    """
    p[0] = {'rules' : p[3], 'after' : p[8], 'window' : timedelta_to_seconds(*p[6])}

def p_receive_rule(p):
    """ receive_stmt : RECEIVE INDENT match_clauses DEDENT
    """
    p[0] = {'clauses' : p[3]}

def p_receive_rule2(p):
    """ receive_stmt : RECEIVE INDENT match_clauses DEDENT AFTER TIMEDELTA ARROW actions """
    p[0] = {'clauses' : p[3], 'window' : timedelta_to_seconds(*p[6]), 'after' : p[8] }

def p_receive_rule3(p):
    """ receive_stmt : RECEIVE INDENT match_clauses DEDENT AFTER ARROW actions """
    p[0] = {'clauses' : p[3], 'after' : p[7] }

def p_match_clauses(p):
    """match_clauses : match_clauses NEWLINE match_clause
                     | match_clause """
    if len(p) > 2:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]

def p_match_clause(p):
    """ match_clause : conditions ARROW actions """
    p[0] = {k:v for k, v in [("attrs", p[1]), ("lineno", p.lineno(2))] + p[3].items()}

def p_match_clause2(p):
    """ match_clause : WILDCARD ARROW actions """
    p[0] = {k:v for k, v in [("attrs", {}), ("lineno", p.lineno(2))] + p[3].items()}


# concatitems([[1],[2]]) -> [1,2]
# concatitems([1,[2]]) -> [1,2]
# concatitems([1]) -> [1]
def concatitems(items):
    assert(items)
    res = []
    for k, v in items:
        if isinstance(v, list):
            res.extend(v)
        else:
            res.append(v)
    return res

def p_conditions(p):
    """conditions : conditions COMMA condition
                  | condition """
    if len(p) > 2:
        p[0] = {k: concatitems(v) for k, v in groupby(sorted(p[1].items() + p[3].items()),
                                                                        key=lambda x: x[0])}
    else:
        p[0] = p[1]

def p_condition(p):
    """ condition : ID EQ STRING
                  | ID EQ SCALAR """
    p[0] = {p[1]: [p[3]]}

def p_condition_hash(p):
    """ condition : ID IN HASH"""
    p[0] = {p[1]: [p[3]]}

def p_condition_ts(p):
    """ condition : ID LT TIMESTAMP
                 | ID GT TIMESTAMP
                 | ID GTE TIMESTAMP
                 | ID LTE TIMESTAMP """
    p[0] = {p[1]: [p[2] + str(p[3])]}

def p_condition_ts_2(p):
    """ condition : ID LT NUMBER
                 | ID GT NUMBER
                 | ID GTE NUMBER
                 | ID LTE NUMBER """
    p[0] = {p[1]: [p[2] + str(p[3])]}

def p_condition_ts_3(p):
    """ condition : ID LT SCALAR
                 | ID GT SCALAR
                 | ID GTE SCALAR
                 | ID LTE SCALAR """
    p[0] = {p[1]: [p[2] + str(p[3])]}

def p_actions(p):
    """ actions : yields COMMA transition """
    p[0] = {'yield' : p[1], 'action' : p[3]}

def p_actions_2(p):
    """ actions : yields """
    p[0] = {'yield' : p[1]}

def p_actions_3(p):
    """ actions : transition """
    p[0] = {'action' : p[1]}

def p_action_yields(p):
    """ yields : yields COMMA YIELD yield_var
               | YIELD yield_var """
    if len(p) == 3:
        p[0] = [p[2]]
    else:
        p[0] = p[1] + [p[4]]

def p_action_yield_var(p):
    """ yield_var : SCALAR_RESULT
    """
    p[0] = {'dst': p[1]}

def p_action_yield_set(p):
    """ yield_var : ID TO HASH """
    p[0] = {'dst': p[3], 'src': [{'_k': 'field', 'name': p[1]}]}

def p_action_yield_multiset(p):
    """ yield_var : ID TO MULTISET """
    p[0] = {'dst': p[3], 'src': [{'_k': 'field', 'name': p[1]}]}

def p_action_yield_hll(p):
    """ yield_var : ID TO HLL """
    p[0] = {'dst': p[3], 'src': [{'_k': 'field', 'name': p[1]}]}

def p_action_yield_set_tuple(p):
    """ yield_var : ids TO HASH """
    p[0] = {'dst': p[3], 'src': p[1]}

def p_action_yield_multiset_tuple(p):
    """ yield_var : ids TO MULTISET """
    p[0] = {'dst': p[3], 'src': p[1]}

def p_action_yield_hll_tuple(p):
    """ yield_var : ids TO HLL """
    p[0] = {'dst': p[3], 'src': p[1]}

def p_ids(p):
    """ids : ids COMMA yieldable
             | yieldable """
    if len(p) > 2:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]

def p_yieldable(p):
    """ yieldable : ID """
    p[0] = {'_k': 'field', 'name': p[1]}

def p_yieldable_start_ts(p):
    """ yieldable : START_TIMESTAMP """
    p[0] = {'_k': 'window_ref'}

def p_yieldable_fcall(p):
    """ yieldable : fcall """
    p[0] = p[1]

def p_yieldable_windowref(p):
    """ yieldable : START_TIMESTAMP LBRACKET ID RBRACKET """
    p[0] = {'_k': 'window_ref', 'ref': p[3]}

def p_fcall(p):
    """ fcall : ID LPAREN arglist RPAREN """
    p[0] = {'_k': 'fcall',
            'name': p[1],
            'args': p[3]}

def p_arglist(p):
    """ arglist : arglist COMMA arg
                | arg """
    if len(p) == 2:
        p[0] = [p[1]]
    elif len(p) == 4:
        p[0] = p[1] + [p[3]]

def p_arg_id(p):
    """ arg : ID """
    p[0] = {'_k': 'field', 'name': p[1]}

def p_arg_scalar(p):
    """ arg : SCALAR """
    p[0] = {'_k': 'param', 'name': p[1]}

def p_arg_fcall(p):
    """ arg : fcall """
    p[0] = p[1]

def p_arg_ts(p):
    """ arg : START_TIMESTAMP LBRACKET ID RBRACKET """
    p[0] = {'_k': 'window_ref', 'ref': p[3]}

def p_arg_literal(p):
    """ arg     : STRING
                | NUMBER """
    p[0] = {'_k': 'literal', 'value': p[1]}

def p_transition(p):
    """ transition : ID """
    p[0] = p[1]

def p_transition2(p):
    """ transition : QUIT
                   | REPEAT"""
    p[0] = p[1]

def p_error(p):
    if p is None or p.lineno == -1:
        raise ParseError(message="Syntax error: unexpected EOF")
    else:
        raise ParseError(message="Syntax error at line %s position %s: %s %s" % (p.lineno, p.lexpos, type_names.get(p.type, p.type), p.value),
                         lineno=p.lineno,
                         lexpos=p.lexpos,
                         type=p.type,
                         value=p.value)


# Convert a structure with nested window() statements to a flat list of rules
# Replace transitions with numeric labels. Use restart-from-next(%label)
# in rule matcher clauses, and restart-from-here(%label) in after actions.
def assign_numeric_labels(rules, n = 0):
    for r in rules:
        r['n'] = n
        n += 1
        if 'rules' in r:
            n = assign_numeric_labels(r['rules'], n)
            r['outer'] = n
    return n

def flatten_rules(rules):
    for r in rules:
        nested = r.get('rules')
        if nested:
            del r['rules']
        yield r
        if nested:
            for r in flatten_rules(nested):
                yield r

reserved_actions = ['repeat', 'quit']

def convert_transitions(rules):
    numeric = {r['name'] : r['n'] for r in rules}

    for r in rules:
        if 'after' in r:
            if 'action' in r['after']:
                action = r['after']['action']
                if action not in reserved_actions:
                    r['after']['action'] = 'restart-from-here(%d)' % numeric[action]
            else:
                r['after']['action'] = 'restart-from-here'

        for c in r.get('clauses', []):
            if 'action' in c:
                action = c['action']
                if action not in reserved_actions:
                    if action not in numeric:
                        raise ParseError(message='Label not found: %s' % action, lineno=c.get('lineno'), lexpos=c.get('lexpos'))
                    c['action'] = 'restart-from-next(%d)' % numeric[action]
            else:
                if r['n'] >= 1:
                    raise ParseError(message='Consider adding repeat here', lineno=c.get('lineno'), lexpos=c.get('lexpos'))
                else:
                    c['action'] = 'repeat'

import ply.yacc as yacc
parser = yacc.yacc()

# Build the lexer
lexer = lex.lex()
lexer = IndentLexer(lexer)
import sys
import json


EXPR_TYPE_CONST = 'const'
EXPR_TYPE_IN_VAR = 'in_var'
EXPR_TYPE_TIMESTAMP_OP_VAR = 'timestamp_op_var'
EXPR_TYPE_TIMESTAMP_OP_CONST = 'timestamp_op_const'

def is_variable(n):
    if n == '':
        return False
    return n[0] in '#&%$@'


def parse_expr(expr_string):
    m = re.match('((>=)|(<=)|(==)|(<)|(>))(.+)', expr_string)
    if m:
        if is_variable(m.group(7)):
            return (EXPR_TYPE_TIMESTAMP_OP_VAR, (m.group(1), m.group(7)))
        else:
            return (EXPR_TYPE_TIMESTAMP_OP_CONST, (m.group(1), m.group(7)))
    if is_variable(expr_string):
        return (EXPR_TYPE_IN_VAR, (expr_string,))
    else:
        return (EXPR_TYPE_CONST, (expr_string,))


def get_var_fields(rules):
    res = {}
    for rule in rules:
        for clause in rule.get('clauses', []):
            for field, conditions in clause.get('attrs', {}).items():
                for expr in conditions:
                    t, r = parse_expr(expr)
                    if t == EXPR_TYPE_IN_VAR:
                        res[r[0]] = field
                    elif t == EXPR_TYPE_TIMESTAMP_OP_VAR:
                        res[r[1]] = field
    return res


def compile_tr(text):
    lexer.input(text)
    result = parser.parse(lexer = lexer)
    assign_numeric_labels(result['rules'])
    flat_rules = list(flatten_rules(result['rules']))
    convert_transitions(flat_rules)

    if 'groupby' in result:
        return { 'rules' : flat_rules, 'groupby': result['groupby']}
    else:
        return {'rules' : flat_rules}

def syntax_check(text):
    try:
        parser = yacc.yacc()

        # Build the lexer
        lexer = lex.lex()
        lexer = IndentLexer(lexer)

        sys.stderr.write("text %s\n" % text)
        lexer.input(text)
        result = parser.parse(lexer=lexer)

        assign_numeric_labels(result['rules'])
        flat_rules = list(flatten_rules(result['rules']))
        convert_transitions(flat_rules)

        return []
    except ParseError as e:
        sys.stderr.write("exception %s %s\n" % (e.message, lexer.lexer.lineno))
        return [{'message' : e.message, 'info' : e.info}]

if __name__ == '__main__':
    if len(sys.argv) == 1:
        flat_rules = compile_tr(sys.stdin.read())
        print(json.dumps(flat_rules))
    elif sys.argv[1] == 'lex':
        lexer.input(sys.stdin.read())
        for t in lexer:
            print(t.lineno, t.type, t.value)
    elif sys.argv[1] == 'gen':
        pass
