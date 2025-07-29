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

def test_sequential_performance(client, keys, bins, iterations=30):
    """Test sequential performance"""
    latencies = []
    
    total_start = time.perf_counter()
    
    for i in range(iterations):
        result = single_batch_read(client, keys, bins)
        if result['success']:
            latencies.append(result['latency'])
    
    total_end = time.perf_counter()
    total_time = (total_end - total_start) * 1000  # ms
    
    return {
        'total_time': total_time,
        'latencies': latencies,
        'successful_requests': len(latencies)
    }

def test_threading_performance(client, keys, bins, iterations=30, max_workers=4):
    """Test threading performance"""
    
    total_start = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(single_batch_read, client, keys, bins) 
                  for _ in range(iterations)]
        
        latencies = []
        for future in futures:
            result = future.result()
            if result['success']:
                latencies.append(result['latency'])
    
    total_end = time.perf_counter() 
    total_time = (total_end - total_start) * 1000  # ms
    
    return {
        'total_time': total_time,
        'latencies': latencies,
        'successful_requests': len(latencies)
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
        'p95': latencies[int(n * 0.95)],
        'min': min(latencies),
        'max': max(latencies)
    }

def run_edge_case_tests():
    """Run various edge case tests"""
    
    # Test configurations
    test_configs = [
        # (keys_per_batch, iterations, thread_counts)
        (10, 50, [1, 2, 4, 8, 16]),      # Small batches
        (100, 30, [1, 2, 4, 8, 16]),     # Medium batches  
        (500, 10, [1, 2, 4, 8, 16]),     # Large batches
        (1000, 5, [1, 2, 4, 8, 16]),     # Very large batches
    ]
    
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
        
        print("=== ThreadExecutor vs Sequential - Edge Case Analysis ===")
        print("Testing various batch sizes and thread counts to find bottlenecks\\n")
        
        for keys_count, iterations, thread_counts in test_configs:
            print(f"🔍 **Testing {keys_count} keys per batch, {iterations} iterations**")
            print("-" * 60)
            
            # Generate test keys
            keys = [('test', 'demo', f'user{i}') for i in range(keys_count)]
            
            # Test sequential first
            seq_results = test_sequential_performance(client, keys, bins, iterations)
            seq_stats = calculate_stats(seq_results['latencies'])
            seq_throughput = seq_results['successful_requests'] * 1000 / seq_results['total_time']
            
            print(f"Sequential:  {seq_results['total_time']:6.1f}ms total, "
                  f"{seq_stats.get('mean', 0):5.2f}ms avg, "
                  f"{seq_throughput:6.0f} req/sec")
            
            # Test various thread counts
            for thread_count in thread_counts:
                thread_results = test_threading_performance(client, keys, bins, iterations, thread_count)
                thread_stats = calculate_stats(thread_results['latencies'])
                thread_throughput = thread_results['successful_requests'] * 1000 / thread_results['total_time']
                
                # Calculate performance ratio
                speedup = seq_results['total_time'] / thread_results['total_time']
                throughput_improvement = thread_throughput / seq_throughput
                
                status = "✅" if speedup > 1.0 else "❌" if speedup < 0.8 else "➡️"
                
                print(f"Threading{thread_count:2d}: {thread_results['total_time']:6.1f}ms total, "
                      f"{thread_stats.get('mean', 0):5.2f}ms avg, "
                      f"{thread_throughput:6.0f} req/sec "
                      f"({speedup:.2f}x speedup) {status}")
            
            print()
            
    except Exception as e:
        print(f"Error during testing: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(run_edge_case_tests()) 
