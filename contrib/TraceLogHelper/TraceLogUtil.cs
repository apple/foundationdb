/*
 * TraceLogUtil.cs
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Magnesium
{
	public static class TraceLogUtil
	{
		public static IEnumerable<Event> IdentifyFailedTestPlans(IEnumerable<Event> events)
		{
			var failedPlans = new Dictionary<string, TestPlan>();
			foreach (var ev in events)
			{
				var tp = ev as TestPlan;
				if (tp == null || tp.TestUID == "")
				{
					yield return ev;
					continue;
				}
				var t = tp as Test;
				if (t == null)
				{
					if (!failedPlans.ContainsKey(tp.TestUID + tp.TraceFile))
					{
						failedPlans.Add(tp.TestUID + tp.TraceFile, tp);
					}
				}
				else
				{
					failedPlans.Remove(tp.TestUID + tp.TraceFile);
					if ((tp.TraceFile != null) && tp.TraceFile.EndsWith("-2.txt")) failedPlans.Remove(tp.TestUID + tp.TraceFile.Split('-')[0] + "-1.txt");
					yield return ev;
				}
			}
			foreach (var p in failedPlans.Values)
				yield return new Test
				{
					Type = "FailedTestPlan",
					Time = p.Time,
					Machine = p.Machine,
					TestUID = p.TestUID,
					TestFile = p.TestFile,
					randomSeed = p.randomSeed,
					Buggify = p.Buggify,
					DeterminismCheck = p.DeterminismCheck,
					OldBinary = p.OldBinary,
					events = new Event[] {
						new Event {
							Severity = Severity.SevWarnAlways,
							Type = "TestNotSummarized",
							Time = p.Time,
							Machine = p.Machine
						}
					}
				};
		}
	}
}
