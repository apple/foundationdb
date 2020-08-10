/*
 * XmlParser.cs
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
using System.Xml;
using System.Xml.Linq;

namespace Magnesium
{
	public static class XmlParser
	{
		static Random r = new Random();

		public static IEnumerable<Event> Parse(System.IO.Stream stream, string file, 
			bool keepOriginalElement = false, double startTime = -1, double endTime = Double.MaxValue,
			double samplingFactor = 1.0)
		{
			using (var reader = XmlReader.Create(stream))
			{
				reader.ReadToDescendant("Trace");
				reader.Read();
				foreach (var xev in StreamElements(reader))
				{
					Event ev = null;
					try
					{
						if (xev.Name == "Event")
							ev = ParseEvent(xev, file, keepOriginalElement, startTime, endTime, samplingFactor);
						else if (xev.Name == "Test")
							ev = ParseTest(xev, file, keepOriginalElement);
						else if (xev.Name == "TestPlan")
							ev = ParseTestPlan(xev, file, keepOriginalElement);
					}
					catch (Exception e)
					{
						throw new Exception(string.Format("Failed to parse XML {0}", xev), e);
					}
					if (ev != null) yield return ev;
				}
			}
		}

		private static Event ParseEvent(XElement xEvent, string file, bool keepOriginalElement, double startTime, double endTime, double samplingFactor)
		{
			if (samplingFactor != 1.0 && r.NextDouble() > samplingFactor)
				return null;

			XAttribute trackLatestAttribute = xEvent.Attribute("TrackLatestType");
			bool rolledEvent = trackLatestAttribute != null && trackLatestAttribute.Value.Equals("Rolled");
			String timeAttribute = (rolledEvent) ? "OriginalTime" : "Time";
			double eventTime = double.Parse(xEvent.Attribute(timeAttribute).Value);

			if (eventTime < startTime || eventTime > endTime)
				return null;

			return new Event {
				Severity = (Severity)int.Parse(xEvent.Attribute("Severity").ValueOrDefault("40")),
				Type = string.Intern(xEvent.Attribute("Type").Value),
				Time = eventTime,
				Machine = string.Intern(xEvent.Attribute("Machine").Value),
				ID = string.Intern(xEvent.Attribute("ID").ValueOrDefault("0")),
				TraceFile = file,
				DDetails = xEvent.Attributes()
					.Where(a=>a.Name != "Type" && a.Name != "Time" && a.Name != "Machine" && a.Name != "ID" && a.Name != "Severity" && (!rolledEvent || a.Name != "OriginalTime"))
					.ToDictionary(a=>string.Intern(a.Name.LocalName), a=>(object)a.Value),
				original = keepOriginalElement ? xEvent : null,
			};
		}

		private static string ValueOrDefault( this XAttribute attr, string def ) {
			if (attr == null) return def;
			else return attr.Value;
		}

		private static TestPlan ParseTestPlan(XElement xTP, string file, bool keepOriginalElement)
		{
			var time = double.Parse(xTP.Attribute("Time").ValueOrDefault("0"));
			var machine = xTP.Attribute("Machine").ValueOrDefault("");
			return new TestPlan
			{
				TraceFile = file,
				Type = "TestPlan",
				Time = time,
				Machine = machine,
				TestUID = xTP.Attribute("TestUID").ValueOrDefault(""),
				TestFile = xTP.Attribute("TestFile").ValueOrDefault(""),
				randomSeed = int.Parse(xTP.Attribute("RandomSeed").ValueOrDefault("0")),
				Buggify = xTP.Attribute("BuggifyEnabled").ValueOrDefault("1") != "0",
				DeterminismCheck = xTP.Attribute("DeterminismCheck").ValueOrDefault("1") != "0",
				OldBinary = xTP.Attribute("OldBinary").ValueOrDefault(""),
				original = keepOriginalElement ? xTP : null,
			};
		}

		private static Test ParseTest(XElement xTest, string file, bool keepOriginalElement)
		{
			var time = double.Parse(xTest.Attribute("Time").ValueOrDefault("0"));
			var machine = xTest.Attribute("Machine").ValueOrDefault("");
			return new Test
			{
				TraceFile = file,
				Type = "Test",
				Time = time,
				Machine = machine,
				TestUID = xTest.Attribute("TestUID").ValueOrDefault(""),
				TestFile = xTest.Attribute("TestFile").ValueOrDefault(""),
				SourceVersion = xTest.Attribute("SourceVersion").ValueOrDefault(""),
				ok = bool.Parse(xTest.Attribute("OK").ValueOrDefault("false")),
				randomSeed = int.Parse(xTest.Attribute("RandomSeed").ValueOrDefault("0")),
				randomUnseed = int.Parse(xTest.Attribute("RandomUnseed").ValueOrDefault("0")),
				SimElapsedTime = double.Parse(xTest.Attribute("SimElapsedTime").ValueOrDefault("0")),
				RealElapsedTime = double.Parse(xTest.Attribute("RealElapsedTime").ValueOrDefault("0")),
				passed = int.Parse(xTest.Attribute("Passed").ValueOrDefault("0")),
				failed = int.Parse(xTest.Attribute("Failed").ValueOrDefault("0")),
				peakMemUsage = long.Parse(xTest.Attribute("PeakMemory").ValueOrDefault("0")),
				Buggify = xTest.Attribute("BuggifyEnabled").ValueOrDefault("1") != "0",
				DeterminismCheck = xTest.Attribute("DeterminismCheck").ValueOrDefault("1") != "0",
				OldBinary = xTest.Attribute("OldBinary").ValueOrDefault(""),
				original = keepOriginalElement ? xTest : null,
				events = xTest.Elements().Select(e => 
					new Event {
						Severity = (Severity)int.Parse(e.Attribute("Severity").ValueOrDefault("0")),
						Type = e.Name.LocalName,
						Time = time,
						Machine = machine,
						DDetails = e.Attributes()
							.Where(a => a.Name != "Type" && a.Name != "Time" && a.Name != "Machine" && a.Name != "Severity")
							.ToDictionary(a => a.Name.LocalName, a => (object)a.Value)
					}).ToArray()
			};
		}

		private static T Try<T>(Func<T> action, Func<Exception, T> onError, Func<bool> isEOF)
		{
			try
			{
				return action();
			}
			catch (Exception e)
			{
				if (isEOF())
					return onError(e);
				else
					throw e;
			}
		}

		private static IEnumerable<XElement> StreamElements(this XmlReader reader)
		{
			while (!reader.EOF)
			{
				if (reader.NodeType == XmlNodeType.Element)
				{
					XElement node = null;
					try
					{
						node = XElement.ReadFrom(reader) as XElement;
					}
					catch (Exception) { break; }
					if (node != null)
						yield return node;
				}
				else
				{
					try
					{
						reader.Read();
					}
					catch (Exception) { break; }
				}
			}
		}
	}
}
