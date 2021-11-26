import os
import sys
from os.path import isfile, join
from os import listdir


def divide_file(filepath, header):
    bills = open(filepath, "rt")

    limit = 200000
    idx = 0
    divide = 0
    jump = 3

    sub_bills = open(f"csvs/{divide}.csv", "wt")
    for row in bills:
        if idx == limit:
            sub_bills.close()

            divide = divide + 1
            sub_bills = open(f"csvs/{divide}.csv", "wt")
            for header_row in header:
                sub_bills.write(header_row)
            idx = 0

        idx = idx + 1
        sub_bills.write(row)

    sub_bills.close()


def get_header(file_name, header_rows):
    my_file = open(file_name, "rt")
    header = []

    idx = 0
    for row in my_file:
        if idx <= int(header_rows):
            header.append(row)
            idx += 1

    return header


if __name__ == "__main__":
    print("[Bigfile-Q loader]")
    csv_file = sys.argv[1]
    sep = sys.argv[2]
    header_idx = sys.argv[3]

    header = get_header(csv_file, header_idx)
    divide_file(csv_file, header)

    dir_path = "./csvs"
    onlyfiles = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    for idx, file in enumerate(onlyfiles):
        print(f"start {idx} file load on Sqlite")

        if idx == 0:
            print("First")
            os.system(f"./main csvs/{file} {sep} {header_idx} 1")
        else:
            print(f"{idx}")
            os.system(f"./main csvs/{file} {sep} {header_idx} 0")

