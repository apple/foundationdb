#!/usr/bin/env python
import os
import sys
import re

# Read old machine-readable status document mr-status-old.rst to get the comment
# Store the comment for each keyword into a dictionary
comment_dict = {};
mr_status_rst_filename = 'documentation/sphinx/source/mr-status-json-old.rst';
with open(mr_status_rst_filename, 'r') as mr_status_rst:
    line = mr_status_rst.readline();
    cnt = 1;
    state = 0;
    while line:
        #print("Line {}: {}".format(cnt, line.strip()))
        line = mr_status_rst.readline();
        cnt += 1;
        if state == 0 and line.strip() == "{":
            state = 1;
            #print "state:", state;
            continue;
        else:
            if state == 1 and "//" in line: # comment line
                items = re.split('"|//', line)
                key = items[1]
                comment = items[len(items)- 1]
                #print items;
                #print "Key:", key, "Comment:", comment
                comment_dict[key] = comment;

#print comment_dict;


# Read Schemas.cpp file to get all status
schema_cpp_filename = 'fdbclient/Schemas.cpp';
schema_doc_rawdata = [];
with open(schema_cpp_filename, 'r') as schema_cpp:
   line = schema_cpp.readline();
   cnt = 1;
   state = 0; # Initial
   while line:
       #print("Line {}: {}".format(cnt, line.strip()))
       line = schema_cpp.readline();
       cnt += 1
       if "const KeyRef JSONSchemas::statusSchema" in line:
           state = 1; # start of the statusSchema
       else:
           if line.startswith('{') and state == 1:
               state = 2;
               continue;

       if state == 2:
           if line.startswith(')statusSchema" R"statusSchema('): # Redundant line
               continue;
           if line.startswith('})statusSchema");'): # The end of status schema
               state = 3;
               break;

           items = re.split('"', line)
           if len(items) >= 2:
               key = items[1]
               if key in comment_dict:
                   line = line.strip('\n');
                   line = line + " //" + comment_dict[key] + "\n";

           schema_doc_rawdata.append(line);


for line in schema_doc_rawdata:
    sys.stdout.write(line);

sys.stdout.flush();





