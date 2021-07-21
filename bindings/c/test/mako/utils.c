#include "utils.h"
#include "mako.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

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
/* len is the buffer size, key length + null */
void genkey(char* str, int num, int rows, int len) {
	int i;
	int rowdigit = digits(rows);
	sprintf(str, KEYPREFIX "%0.*d", rowdigit, num);
	for (i = (KEYPREFIXLEN + rowdigit); i < len - 1; i++) {
		str[i] = 'x';
	}
	str[len - 1] = '\0';
}
