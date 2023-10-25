import sys
import os
import random
import string

def generate_random_data(size_in_bytes):
    # Generate random data of the specified size
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size_in_bytes))

def create_file(file_path, size_in_mb):
    # Calculate size in bytes from MB
    size_in_bytes = size_in_mb * 1024 * 1024
    
    # Generate random data
    random_data = generate_random_data(size_in_bytes)
    
    # Write data to file
    with open(file_path, 'w') as file:
        file.write(random_data)

if __name__ == "__main__":
    try:
        # Get file size from command line argument
        size_in_mb = int(sys.argv[1])
        
        # Get file path from command line argument
        file_path = sys.argv[2]
        
        # Create the file
        create_file(file_path, size_in_mb)
        print(f"File of size {size_in_mb} MB created successfully at {file_path}")
    
    except IndexError:
        print("Usage: python script.py <file_size_in_MB> <file_path>")
    except ValueError:
        print("Invalid input. Please enter a valid number for file size.")
    except Exception as e:
        print(f"An error occurred: {e}")
