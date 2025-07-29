#!/usr/bin/env python3

import fast_aerospike
import numpy as np

# Simple test
print("fast_aerospike module import successful!")
print(f"fast_aerospike version: {getattr(fast_aerospike, '__version__', 'N/A')}")

# Client creation test
try:
    config = {
        'hosts': [('127.0.0.1', 3000)]
    }
    client = fast_aerospike.client(config)
    print("Client creation successful!")
    
    # Check batch_read method existence
    if hasattr(client, 'batch_read'):
        print("batch_read method exists!")
        
        # Define sample keys and bins
        keys = [
            ('test', 'demo', 'user1'),
            ('test', 'demo', 'user2'),
            ('test', 'demo', 'user3')
        ]
        
        bins = [
            ('name', 'U10'),    # Unicode string, max 10 chars  
            ('age', 'i4'),      # 32-bit integer
            ('score', 'f4'),     # 32-bit float
            ('balance', 'f8'),    # 64-bit float
            ('active', '?'),    # boolean
            ('city', 'U20'),    # Unicode string, max 20 chars
            ('category', 'U10'),# Unicode string, max 10 chars
            ('level', 'i2')     # 16-bit integer
        ]

        print(f"Number of test keys: {len(keys)}")
        print(f"Test bins: {bins}")
        
        try:
            # Call batch_read (can test function call even without actual Aerospike server)
            result = client.batch_read(keys, bins)
            print(f"batch_read call successful!")
            print(f"Returned type: {type(result)}")
            
            if isinstance(result, np.ndarray):
                print(f"NumPy array returned successfully!")
                print(f"Array shape: {result.shape}")
                print(f"Array dtype: {result.dtype}")
                print(f"Array dtype names: {result.dtype.names}")
                
                # Test recarray access
                if hasattr(result, 'name'):
                    print("result.name access possible!")
                if hasattr(result, 'age'):
                    print("result.age access possible!")
                if hasattr(result, 'score'):
                    print("result.score access possible!")
                    
                print("numpy structured array test successful!")
            else:
                print(f"Unexpected type returned: {type(result)}")
                
        except Exception as e:
            print(f"Error during batch_read call: {e}")
            print(f"Error type: {type(e)}")
            
    else:
        print("batch_read method not found!")
        
except Exception as e:
    print(f"Client creation failed: {e}")
    print(f"Error type: {type(e)}")

print("\nTest completed!") 