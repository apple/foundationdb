/*
 * zipfian distribution copied from YCSB
 * https://github.com/brianfrankcooper/YCSB
 */

#ifndef ZIPF_H
#define ZIPF_H
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define ZIPFIAN_CONSTANT 0.99

void zipfian_generator3(int min, int max, double zipfianconstant);
void zipfian_generator(int items);
int zipfian_next();

#ifdef __cplusplus
}
#endif

#endif /* ZIPF_H */

