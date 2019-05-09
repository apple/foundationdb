#ifndef MAKO_H
#define MAKO_H
#pragma once

#define VERBOSE_NONE 0
#define VERBOSE_DEFAULT 1
#define VERBOSE_ANNOYING 2
#define VERBOSE_DEBUG 3

/* transaction specification */
#define OP_GETREADVERSION 0
#define OP_GET 1
#define OP_GETRANGE 2
#define OP_SGET 3
#define OP_SGETRANGE 4
#define OP_UPDATE 5
#define OP_INSERT 6
#define OP_CLEAR 7
#define OP_CLEARRANGE 8

#define OP_COMMIT 9
#define MAX_OP 10 /* update this when adding a new operation */

#define OP_COUNT 0
#define OP_RANGE 1

#endif /* MAKO_H */