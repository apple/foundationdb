/*
 * This file defines a set of heper C functions and (mostly) macros used 
 * as a source to generate a wrapper shared library called libcbatracefdb_c.so.
 * The resulting library is used along with LD_PRELOAD to override the functions 
 * of shared library libfdb_c.so in order to trace their calls. 
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *f
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
#include <foundationdb/fdb_c_options.g.h> 
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <mutex>
#include <dlfcn.h>
#include <sys/types.h>

/* 
 * SHARED_OBJECT_PUBLIC: 
 *   A macro defining the visibility attribute to use with symbols, the shared 
 *   library have to expose for dynamic linkage.
 */
#define SHARED_OBJECT_PUBLIC __attribute__ ((visibility ("default")))

/*
 * REPORT_USAGE: 
 *   Macro used to generate code that traces calls for the specified function.
 *   it peforms the following actions:
 *     - increments the usage count of the specified function 
 *     - (if requested) writes en entry into the output (tracing) file   
 */
#define REPORT_USAGE(fctName)                                               \
  ++_count_##fctName;                                                       \
  if (traceFlags & CBATRACE_FLAGS_FCTCOUNT)  {                              \
    xmlWriteEvent(outputFile, 1, "api", #fctName, _count_##fctName.load()); \
  }

/* 
 * CALL_FUNCTION_RET:
 *   Macro used to generate a call to a function with a return type.
 *   It generates code that: 
 *     - calls the specified function
 *     - reports/traces the function call by using the REPORT_USAGE macro 
 *     - returns the resulting value 
 */
#define CALL_FUNCTION_RET(fctName, actualArgs)  \
    auto ret = real_##fctName actualArgs;       \
    REPORT_USAGE(fctName)                       \
    return ret

/*
 * CALL_FUNCTION_VOID:
 *   Macro used to generate a call of a functionswithout a retruning type.
 *   it generates code that: 
 *     - calls the specified function
 *     - reports/traces the function call using the REPORT_USAGE macro 
 */
#define CALL_FUNCTION_VOID(fctName, actualArgs) \
    real_##fctName actualArgs;                  \
    REPORT_USAGE(fctName)

/*
 * MAKE_FUNCTION_COMMON: 
 *   A helper macro used to generate function definition code that exploits 
 *   the LD_PRELOAD trick to override functions calls from library libfdb_c.so.     
 *   This macro is shared by all symbols/functions from fdb_c.cpp. It generates  
 *   code for functions with and without a returning type. 
 */
#define MAKE_FUNCTION_COMMON(retType, fctName, formalArgs, actualArgs, callType)\
typedef retType (*fctName##_t)formalArgs;                                       \
static std::atomic<int64_t> _count_##fctName(0);                                \
extern "C" SHARED_OBJECT_PUBLIC                                                 \
retType fctName formalArgs {                                                    \
    static fctName##_t real_##fctName =                                         \
      reinterpret_cast<fctName##_t>(dlsym(libHandle, #fctName));                \
    if (!real_##fctName) {                                                      \
        printf("Couldn't find %s!\\n", #fctName);                               \
        std::abort();                                                           \
    }                                                                           \
    CALL_FUNCTION_##callType(fctName, actualArgs);                              \
}

/* 
 * MAKE_FUNCTION_RET:
 *   Helper macro that generates definition code for a function WITH a return type. 
 *   It simply uses macro MAKE_FUNCTION_COMMON with RET as a function defintion type.
 */
#define MAKE_FUNCTION_RET(retType, fctName, formalArgs, actualArgs)\
MAKE_FUNCTION_COMMON(retType, fctName, formalArgs, actualArgs, RET)

/*
 * MAKE_FUNCTION_VOID: 
 *    Helper macro that generates code for a function WITHOUT a return type. 
 *   It simply uses macro MAKE_FUNCTION_COMMON with VOID as a function defintion type
 *   (the original function is defined with VOID as a return type).   
 */
#define MAKE_FUNCTION_VOID(retType, fctName, formalArgs, actualArgs)\
MAKE_FUNCTION_COMMON(retType, fctName, formalArgs, actualArgs, VOID)

/*
 * MAKE_FUNCTION_IGNORE:
 *   Helper macro to generate code that ignores a function as it does 
 *   not have implementation in the orignal library. 
 */
 #define MAKE_FUNCTION_IGNORE(retType, fctName, formalArgs, actualArgs)

/*
 * Constructor and descructor to be called before the library is resp. 
 * loaded and unloaded
 */
static void init(void) __attribute__ ((constructor));
static void end(void) __attribute__ ((destructor));

/*
 * Constants 
 *   Mainly environment variables.
 *   The underscore prefix is there because the variable are for internal 
 *   use only and therefore they should not be documented. 
 */
static const char CBATRACE_ENV_TRACE_FLAGS[] = "_CBATRACE_FLAGS";
static const char CBATRACE_ENV_OUTPUT_FILE[] = "_CBATRACE_OUTPUT_FILE";

/*
 * Variables 
 *   Global variables 
 */
static void* libHandle  = NULL;    // handle on the original shared library
static FILE *outputFile = NULL;    // file to write the tracing result
static std::mutex outputFileMutex; // mutex to protect writing into the file

static uint8_t traceFlags  = 0x00; // tracing flags 
#define CBATRACE_FLAGS_SUMMARY  0x00  // (default) always display tracing summary. 
#define CBATRACE_FLAGS_FCTCOUNT 0x01  // trace and display every call with counters  

/*
 * xmlWriteEvent: 
 *   Function used to generate an XML tag string called Event  
 *   with the input parameters and then write the resulting tag into 
 *   the provided output file. 
 *   The event tag has the following (XML) attributes:
 *     - Type : type of the event. A string specified by the caller  
 *     - Name : the name (fctName) of FDB function being traced 
 *     - Count: the number of time the specified function was called   
 *
 *   Exmaple:  
 *     Consider this function call: 
 *     xmlWriteEven(outputFile, 3,  "api", "db_create_database", 1)
 *
 *     The call generages a tag named "Event" for function fdb_create_database()
 *     with a call count of 1. The tag is of type "api" and when displayed
 *     it has an indent of 3, which is the number of spaces before the tag. 
 *     
 *     <Trace>
 *     ...
 *        <Event Type="api" Name="fdb_create_database" Count="1" \>
 *     ...
 *     </Trace> 
 */
static void xmlWriteEvent(FILE *file,                     // output file  
                          const u_int8_t indent,          // indent    
                          const char *type,               // type of the tag    
                          const char *fctName,            // name of the fdb function to report on  
                          const std::int64_t callCount) { // number of times the function was called
  // exclusive access to the output file 
  std::lock_guard<std::mutex> ofmg (outputFileMutex);
  if (file != NULL) {
    fprintf(file, 
            "%*s<Event Type=\"%s\" Name=\"%s\" Count=\"%jd\" \\>\n",
            (indent), "", type, fctName, callCount);
  }
}

/*
 * init - constructor 
 * Init() function used by the constructor  
 * The init() function is called when the library is loaded. 
 * It perfoms the following actins:
 * - load the libfdbc_c.so shared library
 * - checeks whether environment variable "_CTRACE_FLAGS" is set and accordingly 
 *   set the global variable traceFlags.   
 * - checeks whether environment variable "_CTRACE_OUTPUT_FILE"
 *   has been set, then uses it to prepare tracing output into a file.
 *   if not STDOUT will be used.
 */
static void init (void) {
  // load library or abort if not found 
  libHandle = dlopen("libfdb_c.so", RTLD_LAZY);
  if (!libHandle) {                                        
      printf("Couldn't find lib %s!\\n", "libfdb_c.so");
      std::abort();                                     
  }      

  // get the tracing flags env parameter 
  if (getenv(CBATRACE_ENV_TRACE_FLAGS)) {
    traceFlags = atoi(getenv(CBATRACE_ENV_TRACE_FLAGS));
  }

  // check whether an output file (path) has been specified 
  // using environment variable CBATRACE_OUTPUT_FILE. 
  // otherwise use stdout 
  if (getenv(CBATRACE_ENV_OUTPUT_FILE)) {
    const char *outputFileName = getenv(CBATRACE_ENV_OUTPUT_FILE);
    const char *outputMode = "w";

    // open the file 
    outputFile = fopen(outputFileName, outputMode);

    // error out and stop if there is a problem opening the file 
    if (outputFile == NULL) {
      fprintf(stderr, "cbatracefdb_c.g.cpp: Error: Unable to open output file \"%s\"\n", outputFileName);

      exit(1);
    }
  }
  else {		
    // default to stdout 
    outputFile = stdout;
  }

  // the tracing output file content is in XML. It is similar to the XML format used by TraceEvent
  // used by FDB client and server event logging  
  // prepare the output for and write the root xml node
  fprintf(outputFile, "<?xml version=\"1.0\"?>\n");
  fprintf(outputFile, "<Trace>\n");

}