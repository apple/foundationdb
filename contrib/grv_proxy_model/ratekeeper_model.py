#
# ratekeeper.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

import numpy
import rate_model
from priority import Priority

class RatekeeperModel:
    def __init__(self, limit_models):
        self.limit_models = limit_models

    def get_limit(self, time, priority):
        return self.limit_models[priority].get_rate(time)

predefined_ratekeeper = {}

predefined_ratekeeper['default200_batch100'] = RatekeeperModel(
{ 
    Priority.SYSTEM: rate_model.UnlimitedRateModel(), 
    Priority.DEFAULT: rate_model.FixedRateModel(200),
    Priority.BATCH: rate_model.FixedRateModel(100) 
})

predefined_ratekeeper['default_sawtooth'] = RatekeeperModel(
{ 
    Priority.SYSTEM: rate_model.UnlimitedRateModel(), 
    Priority.DEFAULT: rate_model.SawtoothRateModel(10, 200, 1),
    Priority.BATCH: rate_model.FixedRateModel(0) 
})

predefined_ratekeeper['default_uniform_random'] = RatekeeperModel(
{ 
    Priority.SYSTEM: rate_model.UnlimitedRateModel(), 
    Priority.DEFAULT: rate_model.DistributionRateModel(lambda: numpy.random.uniform(10, 200), 1),
    Priority.BATCH: rate_model.FixedRateModel(0) 
})

predefined_ratekeeper['default_trickle'] = RatekeeperModel(
{ 
    Priority.SYSTEM: rate_model.UnlimitedRateModel(), 
    Priority.DEFAULT: rate_model.FixedRateModel(3),
    Priority.BATCH: rate_model.FixedRateModel(0) 
})

predefined_ratekeeper['default1000'] = RatekeeperModel(
{
    Priority.SYSTEM: rate_model.UnlimitedRateModel(),
    Priority.DEFAULT: rate_model.FixedRateModel(1000),
    Priority.BATCH: rate_model.FixedRateModel(500)
})
