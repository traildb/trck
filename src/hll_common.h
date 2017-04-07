#pragma once
//#ifndef __HLL_COMMON_H__
//#define __HLL_COMMON_H__
typedef struct hyperloglog_t {
        uint8_t *M;
        uint32_t m;
      // Precision
             int p; 
} hyperloglog_t;

/* Initialize HLL struct with 2^p registers */
hyperloglog_t *hll_init(int p);
