#include "utils.h"
#include "mako.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* uniform-distribution random */
int urand(int low, int high) {
	double r = rand() / (1.0 + RAND_MAX);
	int range = high - low + 1;
	return (int)((r * range) + low);
}

/* random string */
/* len is the buffer size, must include null */
void randstr(char* str, int len) {
	int i;
	for (i = 0; i < len - 1; i++) {
		str[i] = '!' + urand(0, 'z' - '!'); /* generage a char from '!' to 'z' */
	}
	str[len - 1] = '\0';
}

/* random numeric string */
/* len is the buffer size, must include null */
void randnumstr(char* str, int len) {
	int i;
	for (i = 0; i < len - 1; i++) {
		str[i] = '0' + urand(0, 9); /* generage a char from '!' to 'z' */
	}
	str[len - 1] = '\0';
}

/* return the first key to be inserted */
int insert_begin(int rows, int p_idx, int t_idx, int total_p, int total_t) {
	double interval = (double)rows / total_p / total_t;
	return (int)(round(interval * ((p_idx * total_t) + t_idx)));
}

/* return the last key to be inserted */
int insert_end(int rows, int p_idx, int t_idx, int total_p, int total_t) {
	double interval = (double)rows / total_p / total_t;
	return (int)(round(interval * ((p_idx * total_t) + t_idx + 1) - 1));
}

/* devide val equally among threads */
int compute_thread_portion(int val, int p_idx, int t_idx, int total_p, int total_t) {
	int interval = val / total_p / total_t;
	int remaining = val - (interval * total_p * total_t);
	if ((p_idx * total_t + t_idx) < remaining) {
		return interval + 1;
	} else if (interval == 0) {
		return -1;
	}
	/* else */
	return interval;
}

/* number of digits */
int digits(int num) {
	int digits = 0;
	while (num > 0) {
		num /= 10;
		digits++;
	}
	return digits;
}

/* generate a key for a given key number */
/* prefix is "mako" by default, prefixpadding = 1 means 'x' will be in front rather than trailing the keyname */
/* len is the buffer size, key length + null */
void genkey(char* str, char* prefix, int prefixlen, int prefixpadding, int num, int rows, int len) {
	const int rowdigit = digits(rows);
	const int prefixoffset = prefixpadding ? len - (prefixlen + rowdigit) - 1 : 0;
	char* prefixstr = (char*)alloca(sizeof(char) * (prefixlen + rowdigit + 1));
	snprintf(prefixstr, prefixlen + rowdigit + 1, "%s%0.*d", prefix, rowdigit, num);
	memset(str, 'x', len);
	memcpy(str + prefixoffset, prefixstr, prefixlen + rowdigit);
	str[len - 1] = '\0';
}

/* This is another sorting algorithm used to calculate latency parameters */
/* We moved from radix sort to quick sort to avoid extra space used in radix sort */

#if 0
uint64_t get_max(uint64_t arr[], int n) {
	uint64_t mx = arr[0];
	for (int i = 1; i < n; i++) {
		if (arr[i] > mx) {
			mx = arr[i];
		}
	}
	return mx;
}

void bucket_data(uint64_t arr[], int n, uint64_t exp) {
	// uint64_t output[n];
	int i, count[10] = { 0 };
	uint64_t* output = (uint64_t*)malloc(sizeof(uint64_t) * n);

	for (i = 0; i < n; i++) {
		count[(arr[i] / exp) % 10]++;
	}
	for (i = 1; i < 10; i++) {
		count[i] += count[i - 1];
	}
	for (i = n - 1; i >= 0; i--) {
		output[count[(arr[i] / exp) % 10] - 1] = arr[i];
		count[(arr[i] / exp) % 10]--;
	}
	for (i = 0; i < n; i++) {
		arr[i] = output[i];
	}
	free(output);
}

// The main function is to sort arr[] of size n using Radix Sort
void radix_sort(uint64_t* arr, int n) {
	// Find the maximum number to know number of digits
	uint64_t m = get_max(arr, n);
	for (uint64_t exp = 1; m / exp > 0; exp *= 10) bucket_data(arr, n, exp);
}
#endif

int compare(const void* a, const void* b) {
	const uint64_t* da = (const uint64_t*)a;
	const uint64_t* db = (const uint64_t*)b;

	return (*da > *db) - (*da < *db);
}

// The main function is to sort arr[] of size n using Quick Sort
void quick_sort(uint64_t* arr, int n) {
	qsort(arr, n, sizeof(uint64_t), compare);
}
