#ifndef _FLEX_PROCESS_H
#define _FLEX_PROCESS_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

extern void FlexMain(char* startup_data, size_t startup_data_len)
    __attribute__((noreturn));

#ifdef __cplusplus
}
#endif

#endif /* _FLEX_PROCESS_H */