#!/usr/bin/env python

from __future__ import print_function
import csv
import sys


ACCEPTABLE_MEAN_ERROR = 0.015 # 1.5 percent
ACCEPTABLE_SPIKE = 0.05 # 5 percent


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def mean(values):
    return reduce(lambda x, y: float(x) + float(y), values) / len(values)

def is_error_acceptable(filename):
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        errs = []
        dcs = []
        next(reader, None) # skip the csv header
        for row in reader:
            try:
                id_, hll, dc = tuple([int(a) for a in row])
                relative_err = abs(hll-dc)/float(dc)
                errs.append(relative_err)
                dcs.append(dc)

                if relative_err > ACCEPTABLE_SPIKE and dc > 50:
                    print("In the file %s cardinality for id=%d was estimated with an error above expectations: %f > %f (%d vs %d)"
                        % (filename, id_, relative_err, ACCEPTABLE_SPIKE, dc, hll))
            except ValueError:
                pass

        mean_err = mean(errs)

        if mean_err > ACCEPTABLE_MEAN_ERROR:
            eprint("Mean error value for %s is above the acceptance level: %f > %f"
                % (filename, mean_err, ACCEPTABLE_MEAN_ERROR))
            return False

    return True


if __name__ == "__main__":
    if not len(sys.argv) > 1:
        eprint("List of files is missing!\n"
              "Usage: %s file [file] ..." % sys.argv[0])
        sys.exit(1)

    for filename in sys.argv[1:]:
        if not is_error_acceptable(filename):
            sys.exit(2)

    print("All the errors seem to be OK!")

    sys.exit(0)
