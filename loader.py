import os
import sys
import multiprocessing
from multiprocessing import Process, Pipe
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


def run_load(comm, conn):
    print(comm)
    os.system(comm)

    conn.send("done")
    sys.exit()


if __name__ == "__main__":
    print("[Bigfile-Q loader]")
    core_num = multiprocessing.cpu_count()
    workerConn, observerConn = Pipe()

    csv_file = sys.argv[1]
    sep = sys.argv[2]
    header_idx = sys.argv[3]

    header = get_header(csv_file, header_idx)
    divide_file(csv_file, header)

    dir_path = "./csvs"
    onlyfiles = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]

    # Do init-job (For init database and drop exist table)
    init_job = onlyfiles.pop(0)
    os.system(f"./main csvs/{init_job} {sep} {header_idx} 1")

    # Make exec commend
    commends = []
    for idx, file in enumerate(onlyfiles):
        commends.append(f"./main csvs/{file} {sep} {header_idx} 0")

    # Make batch job unit
    processTable = []
    q = []
    idx = 0
    for idx, commend in enumerate(commends):
        q.append(commend)
        idx = idx + 1
        if idx == core_num:
            processTable.append(q)
            q = []
            idx = 0
    if len(q) > 0:
        processTable.append(q)

    # Run batch job as multi-processing
    q_stack = 0
    for batch in processTable:
        for job in batch:
            process = Process(target=run_load, args=(job, workerConn,))
            process.start()

        # wait batch job unit end
        endFlag = 0
        while True:
            done = observerConn.recv()
            if done == "done":
                endFlag += 1
            if endFlag == q_stack:
                print("Queue Done, in jobQ: ", q_stack)
                break
        q_stack = 0
