'''
@author:Syed Shabih Hasan
@date: July 26, 2017

This file tries to reduce some of the data from the alcohol csv and produce a compressed dataset
'''

import csv
from tqdm import tqdm
import pickle
import argparse


def remove_fields(data_line):
    """
    0 Invoice/Item Number,
    1 Date,
    2 Store Number,
    3 Store Name,
    4 Address,
    5 City,
    6 Zip Code,
    7 Store Location,
    8 County Number,
    9 County,
    10 Category,
    11 Category Name,
    12 Vendor Number,
    13 Vendor Name,
    14 Item Number,
    15 Item Description,
    16 Pack,
    17 Bottle Volume (ml),
    18 State Bottle Cost,
    19 State Bottle Retail,
    20 Bottles Sold,
    21 Sale (Dollars),
    22 Volume Sold (Liters),
    23 Volume Sold (Gallons)
    """
    info_dict = {
        'store': [data_line[2], data_line[3]],
        'address': [data_line[4], data_line[5], data_line[6]],
        'county': [data_line[8], data_line[9]],
        'category': [data_line[10], data_line[11]],
        'vendor': [data_line[12], data_line[13]],
        'item': [data_line[14], data_line[15]]
    }
    new_data_line = [data_line[1], data_line[2], data_line[8], data_line[10], data_line[12], data_line[14],
                     data_line[16], data_line[17], data_line[18], data_line[19], data_line[20], data_line[21]]
    return new_data_line, info_dict


def read_and_format_csv(filename, new_filename, variable_filename):
    all_dict = {'store': {}, 'county': {}, 'category': {}, 'vendor': {}, 'item': {}, 'address': {}}
    f_read = open(filename, "r")
    csv_reader = csv.reader(f_read)
    first = True
    i = 0
    n = 1000
    data_holder = []

    for line in tqdm(csv_reader):
        new_data_line, info_dict = remove_fields(line)
        data_holder.append(new_data_line)
        i += 1

        if first:
            first = False
            continue
        else:
            for category in all_dict:
                master_cat_data = all_dict[category]

                if category is 'address':
                    k = info_dict['store'][0]
                    v = info_dict['address']
                else:
                    k = info_dict[category][0]
                    v = info_dict[category][1]

                if k not in master_cat_data:
                    master_cat_data[k] = v
                    all_dict[category] = master_cat_data

        if 0 == i % n:
            with open(new_filename, "a") as f_write:
                csv_writer = csv.writer(f_write)
                csv_writer.writerows(data_holder)
            data_holder = []
            i = 0

    with open(new_filename, "a") as f_write:
        csv_writer = csv.writer(f_write)
        csv_writer.writerows(data_holder)
    f_read.close()

    with open(variable_filename, "wb") as f:
        pickle.dump(all_dict, f)

    return all_dict


def main():
    print("Argument reading")
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", help="original CSV", required=True)
    parser.add_argument("-n", help="new CSV location", required=True)
    parser.add_argument("-d", help="data dictionary location", required=True)

    args = parser.parse_args()

    filename = args.f
    new_filename = args.n
    variable_filename = args.d

    print("Formatting the CSV")
    read_and_format_csv(filename, new_filename, variable_filename)


if __name__ == "__main__":
    main()
