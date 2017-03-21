#ifndef __SAFEIO_H__
#define __SAFEIO_H__
#include <stdlib.h>

#define __STRINGIZE(x) #x
#define STRINGIZE(x) __STRINGIZE(x)

#define DIE_ON_ERROR(msg)\
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define DIE(msg, ...)\
    do { fprintf(stderr, "FAIL: ");\
         fprintf(stderr, msg, ##__VA_ARGS__);\
         exit(EXIT_FAILURE); } while (0)

#define CHECK(cond, msg, ...)\
    if (!(cond)) { \
        fprintf(stderr, "CHECK failed at " __FILE__ ":" STRINGIZE(__LINE__) " ");\
        fprintf(stderr, msg "\n", ##__VA_ARGS__); \
        exit(EXIT_FAILURE); \
    }
// TDB_ERR_OK is 0 so the above cond check doesn't work
#define CHECK_TDB(cond, msg, ...)\
    if (cond != 0) { \
        fprintf(stderr, "CHECK failed at " __FILE__ ":" STRINGIZE(__LINE__) " ");\
        fprintf(stderr, msg "\n", ##__VA_ARGS__); \
        exit(EXIT_FAILURE); \
    }

#define SAFE_WRITE(ptr, size, path, f)\
    if (fwrite(ptr, size, 1, f) != 1){\
        DIE("Writing to %s failed\n", path);\
    }

#define SAFE_FPRINTF(f, path, fmt, ...)\
    if (fprintf(f, fmt, ##__VA_ARGS__) < 1){\
        DIE("Writing to %s failed\n", path);\
    }

#define SAFE_FREAD(f, path, buf, size)\
    if (fread(buf, size, 1, f) < 1){\
        DIE("Reading from %s failed\n", path);\
    }

#define SAFE_SEEK(f, offset, path)\
    if (fseek(f, offset, SEEK_SET) == -1){\
        DIE("Seeking to %llu in %s failed\n", (unsigned long long)offset, path);\
    }

#define SAFE_TELL(f, val, path)\
    if ((val = ftell(f)) == 1){\
        DIE("Checking file position of %s failed\n", path);\
    }

#define SAFE_CLOSE(f, path)\
    if (fclose(f)){\
        DIE("Closing %s failed\n", path);\
    }

#define SAFE_FLUSH(f, path)\
    if (fflush(f)){\
        DIE("Flushing %s failed\n", path);\
    }

#endif
