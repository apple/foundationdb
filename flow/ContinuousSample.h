#ifndef CONTINUOUSSAMPLE_H
#define CONTINUOUSSAMPLE_H
#pragma once

#include <vector>
#include <algorithm>
#include <cmath>

#include "Platform.h"
#include "IRandom.h"

template <class T>
class ContinuousSample
{
public:
    explicit ContinuousSample(int sampleSize)
      : sampleSize(sampleSize)
      , populationSize(0)
      , sorted(true)
    {
    }

    ContinuousSample<T>& addSample(T sample)
    {
        if (!populationSize)
            _min = _max = sample;
        populationSize++;
        sorted = false;

        if (populationSize <= sampleSize) {
            samples.push_back(sample);
        } else if (g_random->random01() <
                   ((double)sampleSize / populationSize)) {
            samples[g_random->randomInt(0, sampleSize)] = sample;
        }

        _max = std::max(_max, sample);
        _min = std::min(_min, sample);
        return *this;
    }

    double mean()
    {
        if (!samples.size())
            return 0;
        T sum = 0;
        for (int c = 0; c < samples.size(); c++)
            sum += samples[c];
        return (double)sum / samples.size();
    }

    T median() { return percentile(0.5); }

    T percentile(double percentile)
    {
        if (!samples.size() || percentile < 0.0 || percentile > 1.0)
            return T();
        sort();
        int idx = std::floor((samples.size() - 1) * percentile);
        return samples[idx];
    }

    T min() { return _min; }
    T max() { return _max; }

    void clear()
    {
        samples.clear();
        populationSize = 0;
        sorted = true;
        _min = _max = 0; // Doesn't work for all T
    }

private:
    int sampleSize;
    uint64_t populationSize;
    bool sorted;
    std::vector<T> samples;
    T _min, _max;

    void sort()
    {
        if (!sorted && samples.size() > 1)
            std::sort(samples.begin(), samples.end());
        sorted = true;
    }
};

#endif
