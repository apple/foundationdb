/*
 * zipfian distribution copied from YCSB
 * https://github.com/brianfrankcooper/YCSB
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include "zipf.h"

/* global static */
static int items;
static int base;
static double zipfianconstant;
static double alpha, zetan, eta, theta, zeta2theta;
static int countforzeta;
static int allowitemcountdecrease = 0;

/* declarations */
double zetastatic2(int st, int n, double theta, double initialsum);
double zeta2(int st, int n, double theta_val, double initialsum);
double zetastatic(int n, double theta);
double zeta(int n, double theta_val);


double rand_double() {
  return (double)rand() / (double)RAND_MAX;
}


int next_int(int itemcount) {
  double u, uz;
  int ret;

  if (itemcount != countforzeta) {
    zetan = zeta2(countforzeta, itemcount, theta, zetan);
  } else if ((itemcount < countforzeta) && (allowitemcountdecrease)) {
    zetan = zeta(itemcount, theta);
  }
  eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);

  u = rand_double();
  uz = u * zetan;

  if (uz < 1.0) {
    return base;
  }

  if (uz < 1.0 + pow(0.5, theta)) {
    return base + 1;
  }

  ret = base + (int)(itemcount * pow(eta * u - eta + 1, alpha));
  return ret;
}

int zipfian_next() {
  return next_int(items);
}

double zetastatic2(int st, int n, double theta, double initialsum) {
  int i;
  double sum = initialsum;
  for (i = st; i < n; i++) {
    sum += 1 / pow(i + 1, theta);
  }
  return sum;
}

double zeta2(int st, int n, double theta_val, double initialsum) {
  countforzeta = n;
  return zetastatic2(st, n, theta_val, initialsum);
}

double zetastatic(int n, double theta) {
  return zetastatic2(0, n, theta, 0);
}

double zeta(int n, double theta_val) {
  countforzeta = n;
  return zetastatic(n, theta_val);
}

void zipfian_generator4(int min, int max, double _zipfianconstant, double _zetan) {
  items = max - min + 1;
  base = min;
  zipfianconstant = _zipfianconstant;

  theta = zipfianconstant;
  zeta2theta = zeta(2, theta);
  alpha = 1.0 / (1.0 - theta);
  zetan = _zetan;
  countforzeta = items;
  eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);

  zipfian_next();
}

void zipfian_generator3(int min, int max, double zipfianconstant) {
  zipfian_generator4(min, max, zipfianconstant, zetastatic(max - min + 1, zipfianconstant));
}

void zipfian_generator2(int min, int max) {
  zipfian_generator3(min, max, ZIPFIAN_CONSTANT);
}

void zipfian_generator(int items) {
  zipfian_generator2(0, items - 1);
}


#if 0 /* test */
void main() {
  int i = 0;
  int histogram[1000] = { 0 };

  srand(time(0));
  
  zipfian_generator(1000);

  for (i = 0; i < 1000000; i++) {
    int val = next_value();
    //printf("%d\n", val);
    histogram[val]++;
  }

  for (i = 0; i < 1000; i++) {
    printf("%d\n", histogram[i]);
  }
}
#endif
