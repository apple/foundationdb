/*
 * zipfian distribution copied from YCSB
 * https://github.com/brianfrankcooper/YCSB
 */

#ifndef ZIPF_H
#define ZIPF_H
#pragma once

#define ZIPFIAN_CONSTANT 0.99

void zipfian_generator(int items);
int zipfian_next();

#endif /* ZIPF_H */
