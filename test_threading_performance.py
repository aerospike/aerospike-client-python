#!/usr/bin/env python3

import time
import fast_aerospike
import threading
from concurrent.futures import ThreadPoolExecutor
import psutil
import os

def get_memory_usage():
    """Get current memory usage"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return {
        'rss': memory_info.rss / 1024 / 1024,  # MB
        'vms': memory_info.vms / 1024 / 1024,  # MB
        'percent': process.memory_percent()
    }

def single_batch_read(client, keys, bins):
    """Single batch_read operation"""
    try:
        start_time = time.perf_counter()
        result = client.batch_read(keys, bins=bins)
        end_time = time.perf_counter()
        return {
            'success': True,
            'latency': (end_time - start_time) * 1000,  # ms
            'records': len(result) if result is not None else 0
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'latency': 0,
            'records': 0
        }

def test_sequential_performance(client, keys, bins, iterations=50):
    """Test sequential performance (simple for loop)"""
    print(f"\n=== Sequential Performance Test ({iterations} iterations) ===")
    
    latencies = []
    start_memory = get_memory_usage()
    
    total_start = time.perf_counter()
    
    for i in range(iterations):
        result = single_batch_read(client, keys, bins)
        if result['success']:
            latencies.append(result['latency'])
        
        if (i + 1) % 10 == 0:
            print(f"   [{i+1:3d}/{iterations}] Latency: {result['latency']:6.2f}ms")
    
    total_end = time.perf_counter()
    end_memory = get_memory_usage()
    
    total_time = (total_end - total_start) * 1000  # ms
    
    return {
        'total_time': total_time,
        'latencies': latencies,
        'successful_requests': len(latencies),
        'memory_delta': end_memory['rss'] - start_memory['rss']
    }

def test_threading_performance(client, keys, bins, iterations=50, max_workers=4):
    """Test threading performance with concurrent.futures"""
    print(f"\n=== Threading Performance Test ({iterations} iterations, {max_workers} workers) ===")
    
    start_memory = get_memory_usage()
    
    total_start = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = []
        for i in range(iterations):
            future = executor.submit(single_batch_read, client, keys, bins)
            futures.append(future)
        
        # Collect results
        latencies = []
        completed = 0
        for future in futures:
            result = future.result()
            if result['success']:
                latencies.append(result['latency'])
            
            completed += 1
            if completed % 10 == 0:
                print(f"   [{completed:3d}/{iterations}] Latency: {result['latency']:6.2f}ms")
    
    total_end = time.perf_counter()
    end_memory = get_memory_usage()
    
    total_time = (total_end - total_start) * 1000  # ms
    
    return {
        'total_time': total_time,
        'latencies': latencies,
        'successful_requests': len(latencies),
        'memory_delta': end_memory['rss'] - start_memory['rss']
    }

def calculate_stats(latencies):
    """Calculate latency statistics"""
    if not latencies:
        return {}
    
    latencies.sort()
    n = len(latencies)
    
    return {
        'mean': sum(latencies) / n,
        'median': latencies[n//2],
        'p90': latencies[int(n * 0.9)],
        'p95': latencies[int(n * 0.95)],
        'p99': latencies[int(n * 0.99)],
        'min': min(latencies),
        'max': max(latencies)
    }

def main():
    print("=== Threading vs Sequential Performance Test ===")
    
    # Test configuration
    iterations = 50
    max_workers = 4
    
    # Test data
    keys = [('test', 'demo', f'user{i}') for i in range(100)]  # 100 keys per batch
    bins = [
        ('name', 'U10'),
        ('age', 'i4'),
        ('score', 'f4'),
        ('balance', 'f8'),
        ('active', '?'),
        ('city', 'U20'),
        ('category', 'U10'),
        ('level', 'i2')
    ]
    
    try:
        # Create client
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        client = fast_aerospike.client(config)
        print(f"Client created successfully!")
        print(f"Test setup: {len(keys)} keys per batch, {len(bins)} bins")
        
        # Test 1: Sequential performance
        seq_results = test_sequential_performance(client, keys, bins, iterations)
        
        # Test 2: Threading performance  
        thread_results = test_threading_performance(client, keys, bins, iterations, max_workers)
        
        # Analysis
        print(f"\n{'='*60}")
        print("PERFORMANCE COMPARISON RESULTS")
        print(f"{'='*60}")
        
        seq_stats = calculate_stats(seq_results['latencies'])
        thread_stats = calculate_stats(thread_results['latencies'])
        
        print(f"\n--- Sequential Performance ---")
        print(f"Total time:        {seq_results['total_time']:8.2f}ms")
        print(f"Successful:        {seq_results['successful_requests']:8d}/{iterations}")
        print(f"Average latency:   {seq_stats.get('mean', 0):8.2f}ms")
        print(f"Median latency:    {seq_stats.get('median', 0):8.2f}ms")
        print(f"P99 latency:       {seq_stats.get('p99', 0):8.2f}ms")
        print(f"Memory delta:      {seq_results['memory_delta']:8.2f}MB")
        print(f"Throughput:        {(seq_results['successful_requests'] * 1000 / seq_results['total_time']):8.0f} req/sec")
        
        print(f"\n--- Threading Performance ({max_workers} workers) ---")
        print(f"Total time:        {thread_results['total_time']:8.2f}ms")
        print(f"Successful:        {thread_results['successful_requests']:8d}/{iterations}")
        print(f"Average latency:   {thread_stats.get('mean', 0):8.2f}ms")
        print(f"Median latency:    {thread_stats.get('median', 0):8.2f}ms")
        print(f"P99 latency:       {thread_stats.get('p99', 0):8.2f}ms")
        print(f"Memory delta:      {thread_results['memory_delta']:8.2f}MB")
        print(f"Throughput:        {(thread_results['successful_requests'] * 1000 / thread_results['total_time']):8.0f} req/sec")
        
        # Performance comparison
        total_time_ratio = thread_results['total_time'] / seq_results['total_time']
        latency_ratio = thread_stats.get('mean', 0) / seq_stats.get('mean', 1)
        throughput_improvement = (thread_results['successful_requests'] * 1000 / thread_results['total_time']) / (seq_results['successful_requests'] * 1000 / seq_results['total_time'])
        
        print(f"\n--- Performance Analysis ---")
        print(f"Total time ratio:     {total_time_ratio:.2f}x ({'faster' if total_time_ratio < 1 else 'slower'} with threading)")
        print(f"Latency ratio:        {latency_ratio:.2f}x ({'lower' if latency_ratio < 1 else 'higher'} individual latency)")
        print(f"Throughput improvement: {throughput_improvement:.2f}x")
        
        if total_time_ratio > 1.1:
            print(f"\n⚠️  Threading is {total_time_ratio:.1f}x slower than sequential!")
            print("Possible reasons:")
            print("- Thread creation/management overhead")
            print("- Context switching overhead")
            print("- Aerospike client internal synchronization")
            print("- Network connection resource contention")
            print("- Memory allocation/deallocation overhead")
        elif total_time_ratio < 0.9:
            print(f"\n✅ Threading is {1/total_time_ratio:.1f}x faster than sequential!")
            print("GIL release is working effectively for I/O operations")
        else:
            print(f"\n➡️  Threading and sequential performance are similar")
            print("Threading overhead balances with parallelization benefits")
            
    except Exception as e:
        print(f"Error during testing: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main()) 
