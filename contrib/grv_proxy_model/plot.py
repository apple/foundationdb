#
# plot.py
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

import matplotlib.pyplot as plt


class Plotter:
    def __init__(self, results):
        self.results = results

    def add_plot(data, time_resolution, label, use_avg=False):
        out_data = {}
        counts = {}
        for t in data.keys():
            out_data.setdefault(t // time_resolution * time_resolution, 0)
            counts.setdefault(t // time_resolution * time_resolution, 0)
            out_data[t // time_resolution * time_resolution] += data[t]
            counts[t // time_resolution * time_resolution] += 1

        if use_avg:
            out_data = {t: v / counts[t] for t, v in out_data.items()}

        plt.plot(list(out_data.keys()), list(out_data.values()), label=label)

    def add_plot_with_times(data, label):
        plt.plot(list(data.keys()), list(data.values()), label=label)

    def display(self, time_resolution=0.1):
        plt.figure(figsize=(40, 9))
        plt.subplot(3, 3, 1)
        for priority in self.results.started.keys():
            Plotter.add_plot(self.results.started[priority], time_resolution, priority)

        plt.xlabel("Time (s)")
        plt.ylabel("Released/s")
        plt.legend()

        plt.subplot(3, 3, 2)
        for priority in self.results.queued.keys():
            Plotter.add_plot(self.results.queued[priority], time_resolution, priority)

        plt.xlabel("Time (s)")
        plt.ylabel("Requests/s")
        plt.legend()

        plt.subplot(3, 3, 3)
        for priority in self.results.unprocessed_queue_sizes.keys():
            data = {
                k: max(v)
                for (k, v) in self.results.unprocessed_queue_sizes[priority].items()
            }
            Plotter.add_plot(data, time_resolution, priority)

        plt.xlabel("Time (s)")
        plt.ylabel("Max queue size")
        plt.legend()

        num = 4
        for priority in self.results.latencies.keys():
            plt.subplot(3, 3, num)
            median_latencies = {
                k: v[int(0.5 * len(v))] if len(v) > 0 else 0
                for (k, v) in self.results.latencies[priority].items()
            }
            percentile90_latencies = {
                k: v[int(0.9 * len(v))] if len(v) > 0 else 0
                for (k, v) in self.results.latencies[priority].items()
            }
            max_latencies = {
                k: max(v) if len(v) > 0 else 0
                for (k, v) in self.results.latencies[priority].items()
            }

            Plotter.add_plot(median_latencies, time_resolution, "median")
            Plotter.add_plot(percentile90_latencies, time_resolution, "90th percentile")
            Plotter.add_plot(max_latencies, time_resolution, "max")

            plt.xlabel("Time (s)")
            plt.ylabel(str(priority) + " Latency (s)")
            plt.yscale("log")
            plt.legend()
            num += 1

        for priority in self.results.rate.keys():
            plt.subplot(3, 3, num)
            if len(self.results.rate[priority]) > 0:
                Plotter.add_plot(
                    self.results.rate[priority], time_resolution, "Rate", use_avg=True
                )
            if len(self.results.released[priority]) > 0:
                Plotter.add_plot(
                    self.results.released[priority],
                    time_resolution,
                    "Released",
                    use_avg=True,
                )
            if len(self.results.limit[priority]) > 0:
                Plotter.add_plot(
                    self.results.limit[priority], time_resolution, "Limit", use_avg=True
                )
            if len(self.results.limit_and_budget[priority]) > 0:
                Plotter.add_plot(
                    self.results.limit_and_budget[priority],
                    time_resolution,
                    "Limit and budget",
                    use_avg=True,
                )
            if len(self.results.budget[priority]) > 0:
                Plotter.add_plot(
                    self.results.budget[priority],
                    time_resolution,
                    "Budget",
                    use_avg=True,
                )

            plt.xlabel("Time (s)")
            plt.ylabel("Value (" + str(priority) + ")")
            plt.legend()
            num += 1

        plt.show()
