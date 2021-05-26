#!/usr/bin/env python
import argparse
import glob
import gzip
import os.path
import sys
import xml.sax
import heapq
import json

# Usage: ./commit_debug.py trace.xml tracing.json
#        ./commit_debug.py trace.xml.gz tracing.json
#        ./commit_debug.py folder-of-traces/ tracing.json
#
# And then open Chrome, navigate to chrome://tracing , and load the tracing.json

def parse_args():
    args = argparse.ArgumentParser()
    args.add_argument('path')
    args.add_argument('output')
    return args.parse_args()

# When encountering an event with this location, use this as the (b)egin or
# (e)nd of a span with a better given name
locationToPhase = {
        "NativeAPI.commit.Before": [],
        "CommitProxyServer.batcher": [("b", "Commit")],
        "CommitProxyServer.commitBatch.Before": [],
        "CommitProxyServer.commitBatch.GettingCommitVersion": [("b", "CommitVersion")],
        "CommitProxyServer.commitBatch.GotCommitVersion": [("e", "CommitVersion")],
        "Resolver.resolveBatch.Before": [("b", "Resolver.PipelineWait")],
        "Resolver.resolveBatch.AfterQueueSizeCheck": [],
        "Resolver.resolveBatch.AfterOrderer": [("e", "Resolver.PipelineWait"), ("b", "Resolver.Conflicts")],
        "Resolver.resolveBatch.After": [("e", "Resolver.Conflicts")],
        "CommitProxyServer.commitBatch.AfterResolution": [("b", "Proxy.Processing")],
        "CommitProxyServer.commitBatch.ProcessingMutations": [],
        "CommitProxyServer.commitBatch.AfterStoreCommits": [("e", "Proxy.Processing")],
        "TLog.tLogCommit.BeforeWaitForVersion": [("b", "TLog.PipelineWait")],
        "TLog.tLogCommit.Before": [("e", "TLog.PipelineWait")],
        "TLog.tLogCommit.AfterTLogCommit": [("b", "TLog.FSync")],
        "TLog.tLogCommit.After": [("e", "TLog.FSync")],
        "CommitProxyServer.commitBatch.AfterLogPush": [("e", "Commit")],
        "NativeAPI.commit.After": [],
}

class CommitDebugHandler(xml.sax.ContentHandler, object):
    def __init__(self, f):
        self._f = f
        self._f.write('[ ') # Trace viewer adds the missing ] for us
        self._starttime = None
        self._data = dict()

    def _emit(self, d):
        self._f.write(json.dumps(d) + ', ')

    def startElement(self, name, attrs):
        # I've flipped from using Async spans to Duration spans, because
        # I kept on running into issues with trace viewer believeing there
        # is no start or end of an emitted span even when there actually is.

        if name == "Event" and attrs.get('Type') == "CommitDebug":
            if self._starttime is None:
                self._starttime = float(attrs['Time'])

            attr_id = attrs['ID']
            # Trace viewer doesn't seem to care about types, so use host as pid and port as tid
            (pid, tid) = attrs['Machine'].split(':')
            traces = locationToPhase[attrs["Location"]]
            for (phase, name) in traces:
                if phase == "b":
                    self._data[(attrs['Machine'], name)] = float(attrs['Time'])
                else:
                    starttime = self._data.get((attrs['Machine'], name))
                    if starttime is None:
                        return
                    trace = {
                        # ts and dur are in microseconds
                        "ts": (starttime - self._starttime) * 1000 * 1000 + 0.001,
                        "dur": (float(attrs['Time']) - starttime) * 1000 * 1000,
                        "cat": "commit",
                        "name": name,
                        "ph": "X",
                        "pid": pid,
                        "tid": tid }
                    self._emit(trace)


def do_file(args, handler, filename):
    openfn = gzip.open if filename.endswith('.gz') else open
    try:
        with openfn(filename) as f:
            xml.sax.parse(f, handler)
    except xml.sax._exceptions.SAXParseException as e:
        print(e)

def main():
    args = parse_args()

    handler = CommitDebugHandler(open(args.output, 'w'))

    def xmliter(filename):
        for line in gzip.open(filename):
            if line.startswith("<Event"):
                start = line.find("Time=")+6
                end = line.find('"', start)
                tx = float(line[start:end])
                yield (tx, line)

    if os.path.isdir(args.path):
        combined_xml = os.path.join(args.path, 'combined.xml.gz')
        if not os.path.exists(combined_xml):
            files = [xmliter(filename) for filename in glob.glob(os.path.join(args.path, "*.xml.gz"))]
            merged = heapq.merge(*files)
            with gzip.open(combined_xml, 'w') as f:
                f.write('<Trace>')
                for line in merged:
                    f.write(line[1])
                f.write('</Trace>')
        do_file(args, handler, combined_xml)
    else:
        do_file(args, handler, args.path)

    return 0


if __name__ == '__main__':
    sys.exit(main())
