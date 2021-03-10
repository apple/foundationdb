#ifndef FLOW_HASH3_H
#define FLOW_HASH3_H
#pragma once

#include <stdint.h>
#include <stddef.h>

// Prototypes for Bob Jenkins' "lookup3" hash
// See Hash3.c for detailed documentation

extern "C" {

uint32_t hashlittle(const void* key, size_t length, uint32_t initval);

void hashlittle2(const void* key, /* the key to hash */
                 size_t length, /* length of the key */
                 uint32_t* pc, /* IN: primary initval, OUT: primary hash */
                 uint32_t* pb); /* IN: secondary initval, OUT: secondary hash */
}

#endif
