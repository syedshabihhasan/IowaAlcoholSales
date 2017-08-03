import csv
import argparse
from tqdm import tqdm


def check_dataline(org_dataline, processed_dataline):
    to_check_idx = {0: 1, 1: 2, 2: 8, 3: 10, 4: 12, 5: 14, 6: 16, 7: 17, 8: 18, 9: 19, 10: 20, 11: 21}
    for new_idx in to_check_idx:
        old_idx = to_check_idx[new_idx]
        if not org_dataline[old_idx] == processed_dataline[new_idx]:
            return False
    return True


def get_csv_count(filename):
    n = 0
    with open(filename, "r") as f:
        co = csv.reader(f)
        for _ in co:
            n += 1
    return n


def read_csvs(original_filename, processed_filename):
    # check data length
    print("checking length equality")
    n_old = get_csv_count(original_filename)
    n_new = get_csv_count(processed_filename)
    print("Same length: {}, \nOld n = {}\nNew n={}".format(n_old == n_new, n_old, n_new))
    if not n_old == n_new:
        return False, None
    # check the data
    f_org = open(original_filename, "r")
    f_new = open(processed_filename, "r")
    csv_org = csv.reader(f_org)
    csv_new = csv.reader(f_new)
    diff_lines = []
    print("comparing data")
    for data_org, data_proc in tqdm(zip(csv_org, csv_new)):
        if not check_dataline(data_org, data_proc):
            diff_lines.append((data_org, data_proc))
    f_new.close()
    f_org.close()
    print("No. of difference found: {}".format(len(diff_lines)))
    return len(diff_lines) > 0, diff_lines if len(diff_lines) > 0 else []


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-o", help="old file path", required=True)
    parser.add_argument("-n", help="new file path", required=True)

    args = parser.parse_args()
    res, diff_lines = read_csvs(args.o, args.n)

if __name__ == "__main__":
    main()

