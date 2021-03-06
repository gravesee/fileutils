import csv
import operator
import os
from operator import itemgetter
from os.path import split
from types import prepare_class
from typing import Iterable, List
from contextlib import ExitStack
import heapq
from itertools import groupby, product

from joblib import Parallel, delayed


def filename(file: str, prefix: str, i: int) -> str:
    _, ext = os.path.splitext(file)
    return prefix.format(i=i) + ext


def split_file(
    file: str,
    dest: str = None,
    header: bool = True,
    newline: bytes = b"\r\n",
    nbytes: int = 64 * 1024 * 1024,
    prefix: str = "part_{i:03d}",
) -> List[str]:
    """quickly split a file into nparts while preserving full lines"""

    if dest is None:
        dest = "."

    if not os.path.exists(dest):
        os.mkdir(dest)

    i = 0
    files: List[str] = []
    with open(file, "rb") as fin:

        if header:
            bytes = fin.read(nbytes)
            skip = bytes.find(newline)
            pos = skip + len(newline)
        else:
            pos = 0

        while True:
            i += 1
            fname = os.path.join(dest, filename(file, prefix, i))
            fin.seek(pos + nbytes)

            bytes = fin.read(nbytes)
            if bytes == b"":
                with open(fname, "wb") as fout:
                    fin.seek(pos)
                    fout.write(fin.read(nbytes))
                    files.append(fname)
                break

            # get the position of the next newline
            nl = bytes.find(newline) + len(newline)
            # print(pos, pos + nbytes + nl)

            fin.seek(pos)

            with open(fname, "wb") as fout:
                fout.write(fin.read(nbytes + nl))
                files.append(fname)
                pos += nbytes + nl

    return files


def add_newline(lines: Iterable[Iterable[str]], sep: str):
    for line in lines:
        yield sep.join(line)
        yield "\n"


def sort_file(
    file: str,
    col: int,
    dialect: csv.Dialect = csv.excel,
    reverse: bool = False,
    inplace: bool = True,
):
    with open(file, "r") as fin:
        reader = csv.reader(fin, dialect=dialect)
        lines = sorted(reader, key=itemgetter(col), reverse=reverse)

    if not inplace:
        file = file + "-sorted"

    with open(file, "w") as fout:
        fout.writelines(add_newline(lines, dialect.delimiter))


def sort_files(
    files: List[str],
    col: int,
    dialect: csv.Dialect = csv.excel,
    reverse: bool = False,
    inplace: bool = True,
    n_jobs=-1,
):
    Parallel(n_jobs=n_jobs)(
        delayed(sort_file)(file, col, dialect, reverse, inplace) for file in files
    )


def merge_files(
    files: List[str],
    outfile: str,
    col: int,
    dialect: csv.Dialect = csv.excel,
    reverse: bool = False,
):
    with open(outfile, "w") as fout:
        with ExitStack() as stack:
            handles = [stack.enter_context(open(file)) for file in files]
            readers = [csv.reader(handle) for handle in handles]

            lines = heapq.merge(*readers, key=itemgetter(col), reverse=reverse)
            fout.writelines(add_newline(lines, dialect.delimiter))


def disksort(
    infile: str,
    outfile: str,
    col: int,
    header: bool = True,
    newline: bytes = b"\r\n",
    reverse: bool = False,
    dialect: csv.Dialect = csv.excel,
    nbytes: int = 64 * 1024 * 1024,
    n_jobs=-1,
):
    """sort file on disk using nbytes * n_jobs memory"""

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        files = split_file(
            infile, dest=tmp, header=header, newline=newline, nbytes=nbytes
        )
        sort_files(files, col, dialect, reverse, inplace=True, n_jobs=n_jobs)
        merge_files(files, outfile, col, dialect, reverse)


def left_join(left: str, right: str, left_on: int, right_on: int):
    with open(left, "r") as fl, open(right, "r") as fr:
        lsubset = groupby(csv.reader(fl), key=itemgetter(left_on))
        rsubset = groupby(csv.reader(fr), key=itemgetter(right_on))

        lkey, llines = next(lsubset)
        rkey, rlines = next(rsubset)

        try:
            while True:
                if lkey == rkey:
                    for l, r in product(llines, rlines):
                        yield l + r
                    lkey, llines = next(lsubset)
                    rkey, rlines = next(rsubset)
                
                elif lkey < rkey:
                    lkey, llines = next(lsubset)
                
                else:
                    rkey, rlines = next(rsubset)
        except:
            pass


def join(left, right, left_on, right_on, outfile: str):
    res = left_join(left, right, left_on, right_on)
    with open(outfile, "w") as fout:
        fout.writelines(add_newline(res, sep=","))


def checksum_ignore_order(file: str):
    """simple utility func to check that files are the same regardless of row-order"""
    sum = 0
    with open(file, "rb") as fin:
        for bytes in fin:
            sum += hash(bytes)
    return sum
