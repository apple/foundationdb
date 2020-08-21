# fdbcstat
`fdbcstat` is a FoundationDB client monitoring tool which collects and displays transaction operation statistics inside the C API library (`libfdb_c.so`).

## How it works
`fdbcstat` utilizes [eBPF/bcc](https://github.com/iovisor/bcc) to attach to `libfdb_c.so` shared library and insert special instructions to collect statistics in several common `fdb_transaction_*` calls, then it periodically displays the aggregated statistics.

## How to use

### Syntax
`fdbcstat <full path to libfdb_c.so> <options...>`

### Options
- `-p` or `--pid` : Only capture statistics for the functions called by the specified process
- `-i` or `--interval` : Specify the time interval in seconds between 2 outputs (Default: 1)
- `-d` or `--duration` : Specify the total duration in seconds `fdbcstats` will run (Default: Unset / Forever)
- `-f` or `--functions` : Specify the comma-separated list of functions to monitor (Default: Unset / All supported functions)

### Supported Functions
- get
- get_range
- get_read_version
- set
- clear
- clear_range
- commit

### Examples
##### Collect all statistics and display every second
`fdbcstat /usr/lib64/libfdb_c.so`
##### Collect all statistics for PID 12345 for 60 seconds with 10 second interval
`fdbcstat /usr/lib64/libfdb_c.so -p 12345 -d 60 -i 10`
##### Collect statitics only for get and commit
`fdbcstat /usr/lib64/libfdb_c.so -f get,commit`

## Output Format
Each line contains multiple fields.  The first field is the timestamp.  Other fields are the statistics for each operation.  Each operation field contains the following statistics in a slash (/) separated format.

- Function
- Number of calls per second
- Average latency in microseconds (us)
- Maximum latency in microseconds (us)

**Note**: The latency is computed as the time difference between the start time and the end time of the `fdb_transaction_*` function call except for `get`, `get_range`, `get_read_version` and `commit`.  For those 4 functions, the latency is the time difference between the start time of the function and the end time of the following `fdb_future_block_until_ready` call.

## Sample Output
```
...
15:05:31 clear/22426/2/34 commit/18290/859/15977 get/56230/1110/12748 get_range/14141/23/75 set/6276/3/19 
15:05:41 clear/24147/2/38 commit/18259/894/44259 get/57978/1098/15636 get_range/13171/23/90 set/6564/3/15 
15:05:51 clear/21287/2/34 commit/18386/876/17824 get/58318/1106/30539 get_range/13018/23/68 set/6559/3/13 
...
```
