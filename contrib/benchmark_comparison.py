#!/usr/bin/env python3
"""
Generate comprehensive actor vs coroutine benchmark comparison report.

USAGE:
    cd build_output  # or your build directory with bin/flowbench
    python3 ../contrib/benchmark_comparison.py

REQUIREMENTS:
    - Working bin/flowbench executable
    - Both actor and coroutine benchmarks available:
      * bench_net2, coroutine_net2
      * bench_delay.*DELAY.*, coroutine_delay_bench
      * bench_delay.*YIELD.*, coroutine_yield_bench
      * bench_callback, coroutine_callback

OUTPUT:
    Complete comparison report with:
    - DELAY benchmarks (DELAY + YIELD variants, all scales)
    - NET2 benchmarks (allocation-heavy patterns, all scales)
    - CALLBACK benchmarks (various template sizes and scales)
    - OVERALL_GEOMEAN calculations for each section

This tool matches the historical format used in coroutine optimization reports
and provides statistical analysis of performance differences across all
benchmark categories.
"""
import json, math, subprocess, re

def geomean(values):
    if not values: return 0
    return math.exp(sum(math.log(1 + abs(v)) for v in values) / len(values)) - 1

def get_benchmark_data(filter_name):
    """Get benchmark data and parse results"""
    result = subprocess.run(['./bin/flowbench', f'--benchmark_filter={filter_name}', '--benchmark_format=json'], 
                          capture_output=True, text=True, cwd='/root/build_output')
    if result.returncode == 0 and result.stdout.strip():
        return json.loads(result.stdout)['benchmarks']
    return []

def main():
    print("Optimize coroutine final_suspend() - NET2 benchmark performance")
    print("")
    print("Benchmark                                                        Time             CPU      Time Old      Time New       CPU Old       CPU New")
    print("---------------------------------------------------------------------------------------------------------------------------------------------")
    
    # Section 1: DELAY Comparison (DELAY + YIELD) 
    delay_changes = []
    
    # DELAY benchmarks
    delay_actors = get_benchmark_data('bench_delay.*DELAY.*')
    delay_coros = get_benchmark_data('coroutine_delay_bench')
    
    for scale in [0, 1, 8, 64, 512, 4096, 32768, 65536]:
        actor_bench = next((a for a in delay_actors if a['name'].endswith(f'/{scale}')), None)
        coro_bench = next((c for c in delay_coros if c['name'].endswith(f'/{scale}')), None)
        
        if actor_bench and coro_bench:
            time_old, time_new = actor_bench['real_time'], coro_bench['real_time']
            cpu_old, cpu_new = actor_bench['cpu_time'], coro_bench['cpu_time']
            time_change = (time_new - time_old) / time_old
            cpu_change = (cpu_new - cpu_old) / cpu_old
            delay_changes.extend([time_change, cpu_change])
            
            print(f"[bench_delay vs. coroutine_delay]<DELAY>/{scale}                    {time_change:+7.4f}         {cpu_change:+7.4f}      {time_old:>8}      {time_new:>8}       {cpu_old:>8}       {cpu_new:>8}")
    
    # YIELD benchmarks  
    yield_actors = get_benchmark_data('bench_delay.*YIELD.*')
    yield_coros = get_benchmark_data('coroutine_yield_bench')
    
    for scale in [0, 1, 8, 64, 512, 4096, 32768, 65536]:
        actor_bench = next((a for a in yield_actors if a['name'].endswith(f'/{scale}')), None)
        coro_bench = next((c for c in yield_coros if c['name'].endswith(f'/{scale}')), None)
        
        if actor_bench and coro_bench:
            time_old, time_new = actor_bench['real_time'], coro_bench['real_time']
            cpu_old, cpu_new = actor_bench['cpu_time'], coro_bench['cpu_time']
            time_change = (time_new - time_old) / time_old
            cpu_change = (cpu_new - cpu_old) / cpu_old
            delay_changes.extend([time_change, cpu_change])
            
            print(f"[bench_delay vs. coroutine_delay]<YIELD>/{scale}                    {time_change:+7.4f}         {cpu_change:+7.4f}      {time_old:>8}      {time_new:>8}       {cpu_old:>8}       {cpu_new:>8}")
    
    if delay_changes:
        geom = geomean(delay_changes)
        print(f"OVERALL_GEOMEAN                                               {geom:+7.4f}         {geom:+7.4f}             0             0             0             0")
    
    print("")
    print("Comparing bench_net2 to coroutine_net2 (from ./build_output/bin/flowbench)")
    print("Benchmark                                               Time             CPU      Time Old      Time New       CPU Old       CPU New")
    print("----------------------------------------------------------------------------------------------------------------------------")
    
    # Section 2: NET2 Comparison
    net2_actors = get_benchmark_data('bench_net2')
    net2_coros = get_benchmark_data('coroutine_net2')
    net2_changes = []
    
    for actor in net2_actors:
        scale = actor['name'].split('/')[-1]
        coro = next((c for c in net2_coros if c['name'].endswith('/' + scale)), None)
        
        if coro:
            time_old, time_new = actor['real_time'], coro['real_time']
            cpu_old, cpu_new = actor['cpu_time'], coro['cpu_time']
            time_change = (time_new - time_old) / time_old
            cpu_change = (cpu_new - cpu_old) / cpu_old
            net2_changes.extend([time_change, cpu_change])
            
            print(f"[bench_net2 vs. coroutine_net2]/{scale}                    {time_change:+7.4f}         {cpu_change:+7.4f}      {time_old:>8}      {time_new:>8}       {cpu_old:>8}       {cpu_new:>8}")
    
    if net2_changes:
        geom = geomean(net2_changes)
        print(f"OVERALL_GEOMEAN                                      {geom:+7.4f}         {geom:+7.4f}             0             0             0             0")
    
    print("")
    print("Comparing bench_callback to coroutine_callback (from ./build_output/bin/flowbench)")
    print("Benchmark                                                        Time             CPU      Time Old      Time New       CPU Old       CPU New")
    print("---------------------------------------------------------------------------------------------------------------------------------------------")
    
    # Section 3: CALLBACK Comparison
    callback_actors = get_benchmark_data('bench_callback')
    callback_coros = get_benchmark_data('coroutine_callback')
    callback_changes = []
    
    for actor in callback_actors:
        name = actor['name']
        # Parse bench_callback<1>/64 format
        size_match = re.search(r'<(\d+)>', name)
        scale_match = re.search(r'/(\d+)$', name)
        
        if size_match and scale_match:
            size, scale = size_match.group(1), scale_match.group(1)
            coro = next((c for c in callback_coros if f'<{size}>' in c['name'] and c['name'].endswith(f'/{scale}')), None)
            
            if coro:
                time_old, time_new = actor['real_time'], coro['real_time']
                cpu_old, cpu_new = actor['cpu_time'], coro['cpu_time']
                time_change = (time_new - time_old) / time_old
                cpu_change = (cpu_new - cpu_old) / cpu_old
                callback_changes.extend([time_change, cpu_change])
                
                print(f"[bench_callback vs. coroutine_callback]<{size}>/{scale}                  {time_change:+7.4f}         {cpu_change:+7.4f}      {time_old:>8}      {time_new:>8}       {cpu_old:>8}       {cpu_new:>8}")
    
    if callback_changes:
        geom = geomean(callback_changes)
        print(f"OVERALL_GEOMEAN                                               {geom:+7.4f}         {geom:+7.4f}             0             0             0             0")

if __name__ == "__main__":
    main()