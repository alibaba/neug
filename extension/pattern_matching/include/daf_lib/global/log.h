#ifndef GLOBAL_LOG_H_
#define GLOBAL_LOG_H_

#define DAF_LOG_LEVEL 3

#ifdef PRINT_LOG
#define DAF_LOG(level, fmt, ...)                            \
  {                                                         \
    if (level >= DAF_LOG_LEVEL) printf(fmt, ##__VA_ARGS__); \
  }

#else

#define DAF_LOG(level, fmt, ...)

#endif  // PRINT_LOG
#endif  // GLOBAL_LOG_H_
