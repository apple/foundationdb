/*
 * Event.cs
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
using System.Dynamic;

namespace Magnesium
{
    public enum Severity
    {
        SevDebug=5,
        SevInfo=10,
        SevWarn=20,
        SevWarnAlways=30,
        SevError=40,
    };

    public class Event
    {
        public double Time { get; set; }
        public Severity Severity { get; set; }
        public string Type { get; set; }
        public string Machine { get; set; }
        public string ID { get; set; }
        public string WorkerDesc { get { return Machine + " " + ID; } }
        public string TraceFile { get; set; }
        public System.Xml.Linq.XElement original { get; set; }
        static MyExpando emptyDetails = new MyExpando(new Dictionary<string, Object>());
        MyExpando _Details = emptyDetails;
        public dynamic Details { get{ return _Details; } }
        public IDictionary<string,Object> DDetails { get { return _Details._members; }
            set {
                _Details = new MyExpando(value);
                //foreach(var v in value)
                //    _Details.SetMember(v.Key, v.Value);
            }
        }

        public Event ShallowCopy()
        {
            return (Event)MemberwiseClone();
        }

        class MyExpando : DynamicObject
        {
            public MyExpando(IDictionary<string, Object> init)
            {
                _members = init;
            }

            public IDictionary<string, Object> _members =
                    new Dictionary<string, Object>();

            /// <summary>
            /// When a new property is set, 
            /// add the property name and value to the dictionary
            /// </summary>     
            public override bool TrySetMember
                 (SetMemberBinder binder, Object value)
            {
                if (!_members.ContainsKey(binder.Name))
                    _members.Add(binder.Name, value);
                else
                    _members[binder.Name] = value;

                return true;
            }

            public bool SetMember(string name, Object value)
            {
                if (!_members.ContainsKey(name))
                    _members.Add(name, value);
                else
                    _members[name] = value;

                return true;
            }

            /// <summary>
            /// When user accesses something, return the value if we have it
            /// </summary>      
            public override bool TryGetMember
                   (GetMemberBinder binder, out Object result)
            {
                if (_members.ContainsKey(binder.Name))
                {
                    result = _members[binder.Name];
                    return true;
                }
                else
                {
                    return base.TryGetMember(binder, out result);
                }
            }

            /// <summary>
            /// If a property value is a delegate, invoke it
            /// </summary>     
            public override bool TryInvokeMember
               (InvokeMemberBinder binder, Object[] args, out Object result)
            {
                if (_members.ContainsKey(binder.Name)
                          && _members[binder.Name] is Delegate)
                {
                    result = (_members[binder.Name] as Delegate).DynamicInvoke(args);
                    return true;
                }
                else
                {
                    return base.TryInvokeMember(binder, args, out result);
                }
            }


            /// <summary>
            /// Return all dynamic member names
            /// </summary>
            /// <returns>
            public override IEnumerable<string> GetDynamicMemberNames()
            {
                return _members.Keys;
            }
        }

        public string FormatTestError(bool includeDetails)
        {
            string s = Type;
            if (Type == "InternalError")
                s = string.Format("{0} {1} {2}", Type, Details.File, Details.Line);
            else if (Type == "TestFailure")
                s = string.Format("{0} {1}", Type, Details.Reason);
            else if (Type == "ValgrindError")
                s = string.Format("{0} {1}", Type, Details.What);
            else if (Type == "ExitCode")
                s = string.Format("{0} 0x{1:x}", Type, int.Parse(Details.Code));
            else if (Type == "StdErrOutput")
                s = string.Format("{0}: {1}", Type, Details.Output);
            else if (Type == "BTreeIntegrityCheck")
                s = string.Format("{0}: {1}", Type, Details.ErrorDetail);
            if (DDetails.ContainsKey("Error"))
                s += " " + Details.Error;
            if (DDetails.ContainsKey("WinErrorCode"))
                s += " " + Details.WinErrorCode;
            if (DDetails.ContainsKey("LinuxErrorCode"))
                s += " " + Details.LinuxErrorCode;
            if (DDetails.ContainsKey("Status"))
                s += " Status=" + Details.Status;
            if (DDetails.ContainsKey("In"))
                s += " In " + Details.In;
            if (DDetails.ContainsKey("SQLiteError"))
                s += string.Format(" SQLiteError={0}({1})", Details.SQLiteError, Details.SQLiteErrorCode);
            if (DDetails.ContainsKey("Details") && includeDetails)
                s += ": " + Details.Details;

            return s;
        }

    };

    public class TestPlan : Event
    {
        public string TestUID;
        public string TestFile;
        public int randomSeed;
        public bool Buggify;
        public bool DeterminismCheck;
        public string OldBinary;

    };

    public class Test : TestPlan
    {
        public string SourceVersion;
        public double SimElapsedTime;
        public double RealElapsedTime;
        public bool ok;
        public int passed, failed;
        public int randomUnseed;
        public long peakMemUsage;

        public Event[] events;  // Summarized events during the test
    };

    public struct AreaGraphPoint
    {
        public double X { get; set; }
        public double Y { get; set; }
    };

    public struct LineGraphPoint
    {
        public double X { get; set; }
        public double Y { get; set; }
        public object Category { get; set; }
    };

    public class Interval
    {
        public double Begin { get; set; }
        public double End { get; set; }
        public string Category { get; set; }
        public string Color { get; set; }
        public string Detail { get; set; }
        public object Object { get; set; }
    };

    public class MachineRole : Interval
    {
        public string Machine { get; set; }
        public string Role { get; set; }
    };

    public class Location
    {
        public string Name;
        public int Y;
    };

    //Specifies the type of LocationTime being used
    public enum LocationTimeOp
    {
        //Designates a location in the code at a particular time
        Normal = 0,

        //Designates that this LocationTime maps one event ID to another
        MapId
    };

    //Struct which houses a location in the code and a time which that location was hit
    //The ID signifies the particular pass through the code
    //Some of these objects act as special markers used to connect one ID to another
    public struct LocationTime
    {
        public int id;
        public double Time;
        public Location Loc;

        public LocationTimeOp locationTimeOp;

        //If locationTimeOp == MapId, then this will hold the id of the event that should come after the id specified in the id field
        public int childId;
    };
}