import sys
from abc import ABC, abstractmethod
import re
import argparse

class MapleJob(ABC):
    def __init__(self, input_file: str) -> None:
        self.input_file=input_file
    
    @abstractmethod
    def process_file(self):
        pass

class UnitMapleJob(MapleJob):
    def __init__(self, input_file: str) -> None:
        super().__init__(input_file)

    def process_file(self):
        try:
            # Open the input file for reading

            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    line = line.split(',')
                    key, val = line[0].strip(), '_'.join(line[1:]).strip()
                    sys.stdout.write('[' + key + ': ' + val + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()

class FilterMapleJob(MapleJob):
    def __init__(self, input_file: str, line_regex: str) -> None:
        self.line_regex = f'({line_regex})'
        super().__init__(input_file)

    def process_file(self):
        try:
            # Open the input file for reading
            with open(self.input_file, 'r') as file:
                # Read and print each line
                for line in file:
                    line = line.strip()
                    res = re.findall(self.line_regex, line)
                    if len(res) > 0:
                        line = line.split(',')
                        key, val = line[0].strip(), ','.join(line[1:]).strip()
                        sys.stdout.write('[' + key + ': ' + val + ']\n')
        except FileNotFoundError:
            print(f"Error: File '{self.input_file}' not found.")
        except Exception as e:
            print(f"Error: {e}")
        sys.stdout.flush()

# class JoinMapleJob(MapleJob):
#     def __init__(self, input_file: str, line_regex: str) -> None:
#         self.line_regex = f'({line_regex})'
#         super().__init__(input_file)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--inputfile", type=str, default="data.csv")
    parser.add_argument("-t", "--type", type=str, default="unit")
    parser.add_argument("-p", "--pattern", type=str, default="Video,Radio")

    return parser.parse_args()

if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided

    # Get the input file from the command-line arguments
    args = parse_args()
    
    if len(sys.argv) < 4:
        print("Usage: python3 script.py -f input_file -t [unit | filter | join] -p <pattern>")
        sys.exit(1)

    input_file = args.inputfile
    type=args.type
    if type == "unit":
        maple=UnitMapleJob(input_file)
    elif type == "filter":
        regex = args.pattern
        maple=FilterMapleJob(input_file, regex)
    elif type == "join":
        print("not impl yet")
    else:
        print("Usage: python3 script.py input_file -t [unit | filter | join] -p <pattern>")
        sys.exit(1)

    # Process the file
    maple.process_file()
