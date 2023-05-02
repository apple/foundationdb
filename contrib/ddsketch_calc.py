#!/usr/bin/env python3
#
# ddsketch_calc.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import numpy as np
import math as m


# Implements a DDSketch class as desrcibed in:
# https://arxiv.org/pdf/1908.10693.pdf

# This class has methods that use cubic interpolation to quickly compute log
# and inverse log. The coefficients A,B,C as well as correctingFactor are
# all constants used for interpolating.

# The implementation for interpolation was originally seen here in:
# https://github.com/DataDog/sketches-java/
# in the file CubicallyInterpolatedMapping.java


class DDSketch(object):
    A = 6.0 / 35.0
    B = -3.0 / 5.0
    C = 10.0 / 7.0
    EPS = 1e-18
    correctingFactor = 1.00988652862227438516
    offset = 0
    multiplier = 0
    gamma = 0

    def __init__(self, errorGuarantee):
        self.gamma = (1 + errorGuarantee) / (1 - errorGuarantee)
        self.multiplier = (self.correctingFactor * m.log(2)) / m.log(self.gamma)
        self.offset = self.getIndex(1.0 / self.EPS)

    def fastlog(self, value):
        s = np.frexp(value)
        e = s[1]
        s = s[0]
        s = s * 2 - 1
        return ((self.A * s + self.B) * s + self.C) * s + e - 1

    def reverseLog(self, index):
        exponent = m.floor(index)
        d0 = self.B * self.B - 3 * self.A * self.C
        d1 = (
            2 * self.B * self.B * self.B
            - 9 * self.A * self.B * self.C
            - 27 * self.A * self.A * (index - exponent)
        )
        p = np.cbrt((d1 - np.sqrt(d1 * d1 - 4 * d0 * d0 * d0)) / 2)
        significandPlusOne = -(self.B + p + d0 / p) / (3 * self.A) + 1
        return np.ldexp(significandPlusOne / 2, exponent + 1)

    def getIndex(self, sample):
        return m.ceil(self.fastlog(sample) * self.multiplier) + self.offset

    def getValue(self, idx):
        return (
            self.reverseLog((idx - self.offset) / self.multiplier)
            * 2.0
            / (1 + self.gamma)
        )
