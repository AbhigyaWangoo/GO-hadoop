import argparse
import sys

from typing import List, Dict, Tuple

NOINPUT="NONE"

def parse_args(args_list: List[str]):
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfile", type=str, default=NOINPUT)
    
    return parser.parse_args(args_list)

args = parse_args(sys.argv[1:])
inputfile = args.inputfile

if inputfile == NOINPUT:
    print("Usage: python3 juice.py -i <inputfile>")
    exit(0)

def aggregate_keyvals(fname: str) -> Dict[str, str]:
    output={}

    with open(fname, "r", encoding="utf8") as fp:
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
            
def unit_juice(key: str, values: List[str]) -> Tuple[str, int]:
    return key, len(values)

def select_juice(key: str, values: List[str]) -> Tuple[str, int]:
    pass

def join_juice(key: str, values: List[str]) -> Tuple[str, int]:
    pass

key_value_bindings = aggregate_keyvals(inputfile)
for key in key_value_bindings:
    key, value = unit_juice(key, key_value_bindings[key])
    print(f"[{key}: {value}]")