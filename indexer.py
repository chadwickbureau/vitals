"""A simple script to generate an index of people from data.txt entries.
"""

import sys
import os
import fnmatch

import pandas

def find_files(dir, spec):
    for (root, dirs, files) in os.walk(dir):
        for name in files:
            if fnmatch.fnmatch(name, spec):
                yield root, name

def entries(path):
    with open(os.path.join(path, "data.txt")) as f:
        lines = [ x.strip() for x in f.readlines() ]
    lines = [ x for x in lines if x != "" ]
    d = { 'entry_key': path, 'person_key': None }
    for line in lines:
        try:
            key, value = line.split(":", 1)
        except ValueError:
            continue
        key = key.strip()
        value = value.strip()
        if key in [ 'name-last', 'name-first', 'name-given',
                    'birth-date', 'death-date' ]:
            d[key.replace('-', '_')] = value
    return d

def person_index(root):
    df = pandas.DataFrame([ entries(path)
                            for (path, name) in find_files(root, "data.txt") ])
    df['entry_key'] = df['entry_key'].apply(lambda x: x[len(root)+1:])
    return df[[ 'entry_key', 'person_key', 
                'name_last', 'name_first', 'name_given',
                'birth_date', 'death_date' ]]

if __name__ == '__main__':
    try:
        root = sys.argv[1]
    except IndexError:
        root = "."
    df = person_index(root)
    df.to_csv("index.csv", index=False)

