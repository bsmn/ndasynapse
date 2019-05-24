#!/usr/bin/env python3

"""
Program: count_submission_file_types.py

Purpose: Count the number of unique file types (by file extension) in a csv file generated
         by a call to query-nda using the get-submission-files parameter.

Input parameters: csv file name
                  Optional file type (Submission Associated File, Submission
                      data file, etc.). For multi-word file types, surround
                      the parameter with double quotes.

Outputs: Terminal output csv

Execution: count_submission_file_types.py <csv file name>
               --file_type <file type>
"""

import argparse
import pandas as pd
import os
import sys

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=str,
                        help="Name of the csv file containing the submission file data")
    parser.add_argument("--file_type", type=str, default=None,
                        help="value from the file_type field in the csv file")

    args = parser.parse_args()

    try:
        file_data_df = pd.read_csv(args.csv_file)
    except Exception as file_read_err:
        raise Exception(f"{file_read_err}")

    # Get only the records for the specified file type.
    if args.file_type is not None:
        file_data_df = file_data_df.loc[file_data_df['file_type'] == args.file_type]

    # Get the file extensions.
    file_data_df['file_extension'] = file_data_df["file_remote_path"].apply(os.path.basename)
    file_data_df['file_extension'] = file_data_df["file_extension"].str.split(".", n=1).str[1]

    # Get the count of each file extension in the submission.
    file_ext_count = file_data_df.groupby("file_extension")["file_extension"].count()

    if len(file_ext_count) == 0:
        print(f"\nThere are no files with file type {args.file_type}\n")
    else:
        file_ext_count.to_csv(sys.stdout, header=False)

if __name__ == "__main__":
    main()
