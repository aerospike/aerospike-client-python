"""
fast_aerospike batch_read performance measurement test (500 keys)
- Query keys from multiple sets
- Measure p90, p99 latency
- Monitor memory usage with psutil
"""

import time

import aerospike_helpers
import psutil
import numpy as np
import fast_aerospike
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
    print("fast_aerospike batch_read performance measurement test (500 keys)")
    print("=" * 60)
    
    # Test configuration
    keys = generate_test_keys(500)
    bins = [
        ('name', 'U10'),  # Unicode string, max 10 chars
        ('age', 'i4'),  # 32-bit integer
        ('score', 'f4'),  # 32-bit float
        ('balance', 'f8'),  # 64-bit float
        ('active', '?'),  # boolean
        ('city', 'U20'),  # Unicode string, max 20 chars
        ('category', 'U10'),  # Unicode string, max 10 chars
        ('level', 'i2')  # 16-bit integer
    ]
    #bins_name = [b_name for b_name,b_type in bins]
    
    print(f"Test configuration:")
    print(f"   - Number of keys: {len(keys):,}")
    print(f"   - Set types: {len(set(key[1] for key in keys))} (demo, demo2, demo3, demo4, demo5)")
    print(f"   - Number of bins: {len(bins)}")
    print(f"   - Iterations: {iterations}")
    print()
    
    # Create client
    config = {'hosts': [('127.0.0.1', 3000)]}
    client = fast_aerospike.client(config)
    
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
        result = client.batch_read(keys, bins=bins)
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
              f"Records: {len(result):,}")


    
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


if __name__ == "__main__":
    run_performance_test(iterations=100)


# ============================================================
# Performance measurement results
# ============================================================
# Latency statistics:
#    - Mean:               3.22ms
#    - Median (p50):       3.19ms
#    - p90:                5.23ms
#    - p95:                5.23ms
#    - p99:                5.23ms
#    - Min:                1.99ms
#    - Max:                5.23ms
#
# Throughput:
#    - Total throughput:     155,082 records/sec
#    - Average throughput:   155,082 records/sec
#
# Memory usage:
#    - Average before:      35.07MB
#    - Average after:       35.16MB
#    - Average delta:        0.09MB
#    - Max delta:            0.53MB
#    - Memory per 500 keys:  0.09MB


# ============================================================
# Performance measurement results
# ============================================================
# Latency statistics:
#    - Mean:               3.32ms
#    - Median (p50):       3.26ms
#    - p90:                4.08ms
#    - p95:                4.18ms
#    - p99:                4.35ms
#    - Min:                2.41ms
#    - Max:                4.35ms
#
# Throughput:
#    - Total throughput:     150,528 records/sec
#    - Average throughput:   150,528 records/sec
#
# Memory usage:
#    - Average before:      37.27MB
#    - Average after:       37.31MB
#    - Average delta:        0.04MB
#    - Max delta:            0.67MB
#    - Memory per 500 keys:  0.04MB
#
# Data analysis:


# Performance measurement results
# ============================================================
# Latency statistics:
#    - Mean:               2.39ms
#    - Median (p50):       2.35ms
#    - p90:                3.06ms
#    - p95:                3.19ms
#    - p99:                3.37ms
#    - Min:                1.48ms
#    - Max:                3.37ms
#
# Throughput:
#    - Total throughput:     209,596 records/sec
#    - Average throughput:   209,596 records/sec
#
# Memory usage:
#    - Average before:      36.19MB
#    - Average after:       36.22MB
#    - Average delta:        0.03MB
#    - Max delta:            0.66MB
#    - Memory per 500 keys:  0.03MB
#
# Data analysis: