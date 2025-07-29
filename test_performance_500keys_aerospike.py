"""
aerospike batch_read performance measurement test (500 keys)
- Query keys from multiple sets
- Measure p90, p99 latency
- Monitor memory usage with psutil
"""

import time

import aerospike_helpers
import psutil
import numpy as np
import aerospike
from statistics import mean
import gc


def generate_test_keys(count=500):
    """Generate test keys using multiple sets"""
    keys = []
    sets = ["demo", "demo2", "demo3", "demo4", "demo5"]
    
    for i in range(count):
        namespace = "test"
        set_name = sets[i % len(sets)]  # Cycle through multiple sets
        primary_key = f"user{i+1}"
        keys.append((namespace, set_name, primary_key))
    
    return keys


def get_memory_usage():
    """Return current process memory usage (MB)"""
    process = psutil.Process()
    memory_info = process.memory_info()
    return {
        'rss': memory_info.rss / 1024 / 1024,  # MB
        'vms': memory_info.vms / 1024 / 1024,  # MB
        'percent': process.memory_percent()
    }


def calculate_percentiles(latencies):
    """Calculate latency percentiles"""
    latencies_sorted = sorted(latencies)
    n = len(latencies_sorted)
    
    return {
        'p50': latencies_sorted[int(n * 0.5)],
        'p90': latencies_sorted[int(n * 0.9)],
        'p95': latencies_sorted[int(n * 0.95)],
        'p99': latencies_sorted[int(n * 0.99)],
        'min': min(latencies_sorted),
        'max': max(latencies_sorted),
        'mean': mean(latencies_sorted)
    }


def run_performance_test(iterations=100):
    """Execute performance test"""
    print("=" * 60)
    print("aerospike batch_read performance measurement test (500 keys)")
    print("=" * 60)
    
    # Test configuration
    keys = generate_test_keys(500)
    bins = ['name', 'age', 'score', 'balance', 'active', 'city', 'category', 'level']
    
    print(f"Test configuration:")
    print(f"   - Number of keys: {len(keys):,}")
    print(f"   - Set types: {len(set(key[1] for key in keys))} (demo, demo2, demo3, demo4, demo5)")
    print(f"   - Number of bins: {len(bins)}")
    print(f"   - Iterations: {iterations}")
    print()
    
    # Create client
    config = {'hosts': [('127.0.0.1', 3000)]}
    client = aerospike.client(config).connect()
    
    latencies = []
    memory_before_list = []
    memory_after_list = []
    memory_delta_list = []
    
    print("Running performance test...")
    
    for i in range(iterations):
        # Run GC to clean up memory
        gc.collect()
        
        # Measure memory usage (before)
        memory_before = get_memory_usage()
        memory_before_list.append(memory_before)
        
        # Measure batch_read performance
        start_time = time.perf_counter()
        batch_records = client.batch_read(keys, bins, {})
        end_time = time.perf_counter()

        # Measure memory usage (after)
        memory_after = get_memory_usage()
        latency = (end_time - start_time) * 1000  # Convert to ms

        latencies.append(latency)

        memory_after_list.append(memory_after)

        # Calculate memory increase
        memory_delta = memory_after['rss'] - memory_before['rss']
        memory_delta_list.append(memory_delta)

        # Verify results
        print(f"   [{i+1:2d}/{iterations}] Latency: {latency:7.2f}ms, "
              f"Memory: +{memory_delta:6.2f}MB, "
              f"Records: {len([r for r in batch_records if r[1] is not None]):,}")

    
    # Analyze results
    if latencies:
        percentiles = calculate_percentiles(latencies)
        avg_memory_before = mean([m['rss'] for m in memory_before_list])
        avg_memory_after = mean([m['rss'] for m in memory_after_list])
        avg_memory_delta = mean(memory_delta_list)
        max_memory_delta = max(memory_delta_list)
        
        print()
        print("=" * 60)
        print("Performance measurement results")
        print("=" * 60)
        
        print(f"Latency statistics:")
        print(f"   - Mean:            {percentiles['mean']:7.2f}ms")
        print(f"   - Median (p50):    {percentiles['p50']:7.2f}ms")
        print(f"   - p90:             {percentiles['p90']:7.2f}ms")
        print(f"   - p95:             {percentiles['p95']:7.2f}ms")
        print(f"   - p99:             {percentiles['p99']:7.2f}ms")
        print(f"   - Min:             {percentiles['min']:7.2f}ms")
        print(f"   - Max:             {percentiles['max']:7.2f}ms")
        print()
        
        print(f"Throughput:")
        records_per_sec = (len(keys) * iterations) / (sum(latencies) / 1000)
        print(f"   - Total throughput:     {records_per_sec:,.0f} records/sec")
        print(f"   - Average throughput:   {len(keys) / (percentiles['mean'] / 1000):,.0f} records/sec")
        print()
        
        print(f"Memory usage:")
        print(f"   - Average before:  {avg_memory_before:7.2f}MB")
        print(f"   - Average after:   {avg_memory_after:7.2f}MB")
        print(f"   - Average delta:   {avg_memory_delta:7.2f}MB")
        print(f"   - Max delta:       {max_memory_delta:7.2f}MB")
        print(f"   - Memory per 500 keys: {avg_memory_delta:7.2f}MB")
        print()
        
        print(f"Data analysis:")

        
    else:
        print("No successful tests.")

    client.close()


if __name__ == "__main__":
    run_performance_test(iterations=100)


# ============================================================
# Performance measurement results
# ============================================================
# Latency statistics:
#    - Mean:               25.22ms
#    - Median (p50):       24.89ms
#    - p90:                28.19ms
#    - p95:                29.34ms
#    - p99:                30.42ms
#    - Min:                20.45ms
#    - Max:                30.42ms
#
# Throughput:
#    - Total throughput:     19,825 records/sec
#    - Average throughput:   19,825 records/sec
#
# Memory usage:
#    - Average before:      42.34MB
#    - Average after:       42.95MB
#    - Average delta:        0.61MB
#    - Max delta:            1.23MB
#    - Memory per 500 keys:  0.61MB


# ============================================================
# Performance measurement results  
# ============================================================
# Latency statistics:
#    - Mean:               24.18ms
#    - Median (p50):       23.87ms
#    - p90:                27.45ms
#    - p95:                28.23ms
#    - p99:                29.12ms
#    - Min:                19.87ms
#    - Max:                29.12ms
#
# Throughput:
#    - Total throughput:     20,678 records/sec
#    - Average throughput:   20,678 records/sec
#
# Memory usage:
#    - Average before:      43.21MB
#    - Average after:       43.78MB
#    - Average delta:        0.57MB
#    - Max delta:            1.18MB
#    - Memory per 500 keys:  0.57MB
#
# Data analysis: