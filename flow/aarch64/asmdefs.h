/*
 * Macros for asm code.
 *
 * Copyright (c) 2019-2020, Arm Limited.
 * SPDX-License-Identifier: MIT
 */

#ifndef _ASMDEFS_H
#define _ASMDEFS_H

#if defined(__aarch64__)

/* Branch Target Identitication support.  */
#define BTI_C		hint	34
#define BTI_J		hint	36
/* Return address signing support (pac-ret).  */
#define PACIASP		hint	25; .cfi_window_save
#define AUTIASP		hint	29; .cfi_window_save

/* GNU_PROPERTY_AARCH64_* macros from elf.h.  */
#define FEATURE_1_AND 0xc0000000
#define FEATURE_1_BTI 1
#define FEATURE_1_PAC 2

/* Add a NT_GNU_PROPERTY_TYPE_0 note.  */
#define GNU_PROPERTY(type, value)	\
  .section .note.gnu.property, "a";	\
  .p2align 3;				\
  .word 4;				\
  .word 16;				\
  .word 5;				\
  .asciz "GNU";				\
  .word type;				\
  .word 4;				\
  .word value;				\
  .word 0;				\
  .text

/* If set then the GNU Property Note section will be added to
   mark objects to support BTI and PAC-RET.  */
#ifndef WANT_GNU_PROPERTY
#define WANT_GNU_PROPERTY 1
#endif

#if WANT_GNU_PROPERTY
/* Add property note with supported features to all asm files.  */
GNU_PROPERTY (FEATURE_1_AND, FEATURE_1_BTI|FEATURE_1_PAC)
#endif

#define ENTRY_ALIGN(name, alignment)	\
  .global name;		\
  .type name,%function;	\
  .align alignment;		\
  name:			\
  .cfi_startproc;	\
  BTI_C;

#else

#define END_FILE

#define ENTRY_ALIGN(name, alignment)	\
  .global name;		\
  .type name,%function;	\
  .align alignment;		\
  name:			\
  .cfi_startproc;

#endif

#define ENTRY(name)	ENTRY_ALIGN(name, 6)

#define ENTRY_ALIAS(name)	\
  .global name;		\
  .type name,%function;	\
  name:

#define END(name)	\
  .cfi_endproc;		\
  .size name, .-name;

#define L(l) .L ## l

#ifdef __ILP32__
  /* Sanitize padding bits of pointer arguments as per aapcs64 */
#define PTR_ARG(n)  mov w##n, w##n
#else
#define PTR_ARG(n)
#endif

#ifdef __ILP32__
  /* Sanitize padding bits of size arguments as per aapcs64 */
#define SIZE_ARG(n)  mov w##n, w##n
#else
#define SIZE_ARG(n)
#endif

#endif
