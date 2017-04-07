#pragma once

typedef struct hyperloglog_t {
        uint8_t *M;
        uint32_t m;
      // Precision
             int p; 
} hyperloglog_t;

/* Initialize HLL struct with 2^p registers */
extern hyperloglog_t *hll_init(int p);
