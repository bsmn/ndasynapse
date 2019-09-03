#!/usr/bin/env python3

"""
Program: count_guid_files_by_sampdesc.py

Purpose: Uses the output sample file from manifest_guid_data.py to break down 
         the totals of each file type for the given experiment by sample
         description.

Input parameters: Input file
                  Experiment ID

Outputs: stdout

Execution: count_guid_files_by_sampdesc.py <GUID sample file> <experiment ID>
"""

import argparse
import pandas as pd
import sys

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("guid_input_file", type=argparse.FileType("r"),
                        help="Sample file created by manifest_guid_data.py")
    parser.add_argument("experiment_id", type=int,
                        help="Experiment ID")

    args = parser.parse_args()

    sampdesc_files_df = pd.DataFrame(columns=["data_file_type", "sample_description"])

    data_type_cols = ["data_file1_type", "data_file2_type", "data_file3_type", "data_file4_type"]

    try:
        file_data_df = pd.read_csv(args.guid_input_file)
    except Exception as file_read_error:
        raise file_read_error

    # Convert the column labels to lower case.
    file_data_df.columns = file_data_df.columns.str.lower()

    # Grab the records for the specified experiment ID.
    exp_data_df = file_data_df.loc[file_data_df["experiment_id"] == args.experiment_id]
    if len(exp_data_df) == 0:
        raise Exception(f"There are no files for experiment ID {args.experiment_id}")

    for type_col in data_type_cols:
        data_files_df = exp_data_df[[type_col, "sample_description"]]
        data_files_df = data_files_df.rename(columns={type_col: "data_file_type"})
        sampdesc_files_df = sampdesc_files_df.append(data_files_df)

    # Count the number of data file types by sample description.
    sampdesc_file_count = sampdesc_files_df.groupby(["sample_description", "data_file_type"])["data_file_type"].count()

    sampdesc_file_count.to_csv(sys.stdout, header=False)


if __name__ == "__main__":
    main()

# End of Program #
