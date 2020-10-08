#ifndef UTILS_H
#define UTILS_H
#pragma once

#include <stdint.h>

/* uniform-distribution random */
/* return a uniform random number between low and high, both inclusive */
int urand(int low, int high);

/* write a random string of the length of (len-1) to memory pointed by str
 * with a null-termination character at str[len-1].
 */
void randstr(char* str, int len);

/* write a random numeric string of the length of (len-1) to memory pointed by str
 * with a null-termination character at str[len-1].
 */
void randnumstr(char* str, int len);

/* given the total number of rows to be inserted,
 * the worker process index p_idx and the thread index t_idx (both 0-based),
 * and the total number of processes, total_p, and threads, total_t,
 * returns the first row number assigned to this partition.
 */
int insert_begin(int rows, int p_idx, int t_idx, int total_p, int total_t);

/* similar to insert_begin, insert_end returns the last row numer */
int insert_end(int rows, int p_idx, int t_idx, int total_p, int total_t);

/* devide a value equally among threads */
int compute_thread_portion(int val, int p_idx, int t_idx, int total_p, int total_t);

/* similar to insert_begin/end, compute_thread_tps computes
 * the per-thread target TPS for given configuration.
 */
#define compute_thread_tps(val, p_idx, t_idx, total_p, total_t)                                                        \
	compute_thread_portion(val, p_idx, t_idx, total_p, total_t)

/* similar to compute_thread_tps,
 * compute_thread_iters computs the number of iterations.
 */
#define compute_thread_iters(val, p_idx, t_idx, total_p, total_t)                                                      \
	compute_thread_portion(val, p_idx, t_idx, total_p, total_t)

/* get the number of digits */
int digits(int num);

/* generate a key for a given key number */
/* len is the buffer size, key length + null */
void genkey(char* str, int num, int rows, int len);

#if 0
// The main function is to sort arr[] of size n using Radix Sort
void radix_sort(uint64_t arr[], int n);
void bucket_data(uint64_t arr[], int n, uint64_t exp);
uint64_t get_max(uint64_t arr[], int n);
#endif

// The main function is to sort arr[] of size n using Quick Sort
void quick_sort(uint64_t arr[], int n);
int compare(const void* a, const void* b);

#endif /* UTILS_H */
