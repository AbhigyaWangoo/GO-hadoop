import sys

def process_file(input_file):
    try:
        # Open the input file for reading
        with open(input_file, 'r') as file:
            # Read and print each line
            for line in file:
                line = line.strip()
                line = line.split(' ')
                line[0], line[1] = line[0].strip(), line[1].strip()
                print('[' + line[0] + ': ' + line[1] + ']')
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided
    if len(sys.argv) < 2:
        print("Usage: python script.py input_file [additional_arguments]")
        sys.exit(1)

    # Get the input file from the command-line arguments
    input_file = sys.argv[1]

    # Process the file
    process_file(input_file)
