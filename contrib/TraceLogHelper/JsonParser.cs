/*
 * JsonParser.cs
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
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Xml;
using System.Xml.XPath;
using System.Xml.Linq;

namespace Magnesium
{
	public static class JsonParser
	{
		static Random r = new Random();

		// dummy parameter nonFatalParseError to match xml
		public static IEnumerable<Event> Parse(System.IO.Stream stream, string file,
			bool keepOriginalElement = false, double startTime = -1, double endTime = Double.MaxValue,
			double samplingFactor = 1.0, Action<string> nonFatalErrorMessage = null)
		{
			using (var reader = new System.IO.StreamReader(stream))
			{
				string line;
				while((line = reader.ReadLine()) != null)
				{
					// Remove invalid characters only if it exists in the key
					HashSet<char> invalidChars = new HashSet<char>(":()");
					bool inQuotes = false;
					bool isKey = true;
					StringBuilder newLine = new StringBuilder();
					foreach (char c in line)
					{
						if (c == '"') {
							inQuotes = !inQuotes;
						}
						// if not in the quotes, the colon is the beginning of the value part
						if (!inQuotes && c == ':') {
							isKey = false;
						}
						// if not in the quotes, the comma is the begiining of the next key
						if (!inQuotes && c == ',') {
							isKey = true;
						}
						// remove invalid characters in the key name
						if (isKey && inQuotes && invalidChars.Contains(c)) {
							continue;
						} else {
							newLine.Append(c);
						}
					}
					line = newLine.ToString();

					XElement root = XElement.Load(JsonReaderWriterFactory.CreateJsonReader(new MemoryStream(Encoding.UTF8.GetBytes(line)), new XmlDictionaryReaderQuotas()));
					Event ev = null;
					try
					{
						ev = ParseEvent(root, file, keepOriginalElement, startTime, endTime, samplingFactor);
					}
					catch (Exception e)
					{
						throw new Exception(string.Format("Failed to parse JSON {0}", root), e);
					}
					if (ev != null) yield return ev;
				}
			}
		}

		private static Event ParseEvent(XElement xEvent, string file, bool keepOriginalElement, double startTime, double endTime, double samplingFactor)
		{
			if (samplingFactor != 1.0 && r.NextDouble() > samplingFactor)
				return null;

			XElement trackLatestElement = xEvent.XPathSelectElement("//TrackLatestType");
			bool rolledEvent = trackLatestElement != null && trackLatestElement.Value.Equals("Rolled");
			String timeElement = (rolledEvent) ? "OriginalTime" : "Time";
			double eventTime = double.Parse(xEvent.XPathSelectElement("//" + timeElement).Value);
			
			if (eventTime < startTime || eventTime > endTime)
				return null;

			return new Event {
				Severity = (Severity)int.Parse(xEvent.XPathSelectElement("//Severity").ValueOrDefault("40")),
				Type = string.Intern(xEvent.XPathSelectElement("//Type").Value),
				Time = eventTime,
				Machine = string.Intern(xEvent.XPathSelectElement("//Machine").Value),
				ID = string.Intern(xEvent.XPathSelectElement("//ID").ValueOrDefault("0")),
				TraceFile = file,
				DDetails = xEvent.Elements()
					.Where(a=>a.Name != "Type" && a.Name != "Time" && a.Name != "Machine" && a.Name != "ID" && a.Name != "Severity" && (!rolledEvent || a.Name != "OriginalTime"))
					.ToDictionary(a=>string.Intern(a.Name.LocalName), a=>(object)a.Value),
				original = keepOriginalElement ? xEvent : null
			};
		}

		private static string ValueOrDefault( this XElement attr, string def ) {
			if (attr == null) return def;
			else return attr.Value;
		}
	}
}
