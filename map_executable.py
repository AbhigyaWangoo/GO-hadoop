import sys
from abc import ABC, abstractmethod
import re
import argparse

NOFILE="NOFILE"

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

    parser.add_argument("-f", "--inputfile", type=str, default="NOFILE")
    parser.add_argument("-p", "--pattern", type=str, default="Video,Radio")

    return parser.parse_args()

if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided

    # Get the input file from the command-line arguments
    args = parse_args()
    

    input_file = args.inputfile
    
    if input_file == NOFILE:
        print("Usage: ./maplexec -f input_file -p <pattern>")
        sys.exit(1)

    # maple=UnitMapleJob(input_file)
    regex = args.pattern
    maple=FilterMapleJob(input_file, regex)

    # Process the file
    maple.process_file()
