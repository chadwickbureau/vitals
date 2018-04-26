import glob

import pandas as pd

def parse_file(fn):
    with open(fn) as f:
        it = iter(f)
        record = None
        
        try:
            while True:
                line = it.next().strip()
                if line == "": continue
                record = {'record-type': line.split("-")[0],
                          'source': "/".join(fn.split("/")[:-1])}
                record[",".join(line.split(":")[0].split("-")[1:])] = line.split(":")[1].strip()
                while line != "":
                    line = it.next().strip()
                    if line == "": break
                    record[",".join(line.split(":")[0].split("-")[1:])] = line.split(":")[1].strip()
                yield record
                record = None
        except StopIteration:
            if record is not None:
                yield record
                record = None
            

def main():
    dflist = [ ]
    for fn in glob.glob("*/*/data.txt") + glob.glob("*/*/*/data.txt"):
        print fn
        dflist.append(pd.DataFrame(parse_file(fn)))
    print
    
    df = pd.concat(dflist, ignore_index=True)
    if 'person' not in df:
        df['person'] = None
    df['person'] = df['person'].fillna('_subject')
    for (record_type, data) in df.groupby('record-type'):
        print "Writing %s records" % record_type
        data = data.dropna(axis=1, how='all')
        del data['record-type']
        data.to_csv("processed/%s.csv" % record_type, index=False)

            
if __name__ == '__main__':
    main()
