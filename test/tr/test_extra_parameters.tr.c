#include <stdio.h>
#include <string.h>

#define MAX(x,y) (((x) > (y)) ? (x) : (y))
#define MIN(x,y) (((x) < (y)) ? (x) : (y))

int tu_concat(char *out_var, int out_var_len, char *arg_0, int arg_0_len, char *arg_1, int arg_1_len) {
    int len_0 = MIN(out_var_len, arg_0_len);
    memcpy(out_var, arg_0, len_0);
    int len_1 = MIN(out_var_len - len_0, arg_1_len);
    memcpy(out_var + len_0, arg_1, len_1);
    return len_0 + len_1;
}
