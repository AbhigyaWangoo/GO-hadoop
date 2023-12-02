import argparse
import sys
from abc import ABC, abstractmethod

from typing import List, Dict, Tuple

NOINPUT="NONE"

UNIT="unit"
FILTER="filter"
JOIN="join"

def parse_args(args_list: List[str]):
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfile", type=str, default=NOINPUT)
    parser.add_argument("-t", "--type", type=str, default=NOINPUT)
    
    return parser.parse_args(args_list)

class JuiceTask(ABC):
    def __init__(self, filename: str) -> None:
        self.filename = filename

    def get_key_value_pairs(self) -> Dict[str, List[str]]:
        output={}
        with open(self.filename, "r", encoding="utf8") as fp:
            while True:
                line = fp.readline()[:-1]
                modified_string = line.replace('[', '').replace(']', '').replace(':', '')
                keyval = modified_string.split(" ")

                try:
                    key, value = keyval[0], keyval[1]
                
                    if key in output:
                        output[key] = output[key] + [value]
                    else:
                        output[key] = [value]

                except IndexError: # Means we no longer have line data
                    return output

    def output_task(self):
        key_value_bindings = self.get_key_value_pairs()
        for key in key_value_bindings:
            self.juice_operation(key, key_value_bindings[key])
    
    @abstractmethod
    def juice_operation(self, key: str, values: List[str]):
        pass

class UnitJuiceTask(JuiceTask):
    def __init__(self, filename: str) -> None:
        super().__init__(filename)
    
    def juice_operation(self, key: str, values: List[str]) -> Tuple[str, int]:
        print(f"[{key}: {len(values)}]")

class FilterJuiceTask(JuiceTask):
    def __init__(self, filename: str) -> None:
        super().__init__(filename)
    
    def juice_operation(self, key: str, values: List[str]) -> Tuple[str, int]:
        for value in values:
            print(f"[{key}: {value}]")

class JoinJuiceTask(JuiceTask):
    def __init__(self, filename: str) -> None:
        super().__init__(filename)
    
    def juice_operation(self, key: str, values: List[str]) -> Tuple[str, int]:
        for value in values:
            print(f"[{key}: {value}]")

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    inputfile = args.inputfile
    juicetype = args.type

    if inputfile == NOINPUT or juicetype == NOINPUT:
        print("Usage: python3 juice.py -i <inputfile> -t [unit | filter | join]")
        exit(0)

    if juicetype == UNIT:
        task = UnitJuiceTask(inputfile)
    elif juicetype == FILTER:
        task = FilterJuiceTask(inputfile)
    elif juicetype == JOIN:
        task = JoinJuiceTask(inputfile)

    task.output_task()