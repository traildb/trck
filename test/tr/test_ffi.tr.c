#include <stdio.h>
#include <string.h>


int foo1(char *out_var, int out_var_len, char *arg_0, int arg_0_len) {
    fprintf(stderr, "FOO1 %s\n", arg_0);
    strcpy(out_var, "test");
    return 4;
}

int foo(char *out_var, int out_var_len,
         char *arg_0, int arg_0_len,
         char *arg_1, int arg_1_len,
         char *arg_2, int arg_2_len,
         char *arg_3, int arg_3_len,
         char *arg_4, int arg_4_len,
         char *arg_5, int arg_5_len) {
    fprintf(stderr, "FOO %s %s %s %s %s %s\n", arg_0, arg_1, arg_2, arg_3, arg_4, arg_5);
    strcpy(out_var, "test");
    return 4;
}

#define MIN(x,y) (((x) < (y)) ? (x) : (y))

int inner_fn(char *out_var, int out_var_len, char *arg_0, int arg_0_len) {
    int part1_len = MIN(strlen("inner_"), out_var_len);
    memcpy(out_var, "inner_", part1_len);
    int part2_len = MIN(arg_0_len, out_var_len - part1_len);
    memcpy(out_var + part1_len, arg_0, part2_len);
    return part2_len + part1_len;
}

int outer_fn(char *out_var, int out_var_len, char *arg_0, int arg_0_len) {
    int part1_len = MIN(strlen("inner_"), out_var_len);
    memcpy(out_var, "outer_", part1_len);
    int part2_len = MIN(arg_0_len, out_var_len - part1_len);
    memcpy(out_var + part1_len, arg_0, part2_len);
    return part2_len + part1_len;
}
