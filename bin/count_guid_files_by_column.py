#!/usr/bin/env python3

"""
Program: count_guid_files_by_column.py

Purpose: Uses the output sample file from manifest_guid_data.py to break down 
         the totals of each file type for the given experiment by the specified
         column.

Input parameters: Input file
                  Experiment ID
                  Optional GUID to be removed from the count. It is assumed that
                     the GUID column name is "subjectkey".

Outputs: stdout

Execution: count_guid_files_by_sampdesc.py -h
"""

import argparse
import pandas as pd
import sys

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("guid_input_file", type=argparse.FileType("r"),
                        help="Sample file created by manifest_guid_data.py")
    parser.add_argument("experiment_id", type=int, help="Experiment ID")
    parser.add_argument("column_name", type=str, help="Column name")
    parser.add_argument("--remove_guid", type=str,
                          help="GUID to remove from the count")

    args = parser.parse_args()

    sampdesc_files_df = pd.DataFrame(columns=["data_file_type", args.column_name])

    data_type_cols = ["data_file1_type", "data_file2_type", "data_file3_type", "data_file4_type"]

    file_data_df = pd.read_csv(args.guid_input_file)

    # Convert the column labels to lower case.
    file_data_df.columns = file_data_df.columns.str.lower()

    # If a GUID has been specified to remove from the data, remove it.
    if args.remove_guid is not None:
        file_data_df = file_data_df.loc[file_data_df["subjectkey"] != args.remove_guid]

    # Grab the records for the specified experiment ID.
    exp_data_df = file_data_df.loc[file_data_df["experiment_id"] == args.experiment_id]
    if len(exp_data_df) == 0:
        raise Exception(f"There are no files for experiment ID {args.experiment_id}")

    for type_col in data_type_cols:
        data_files_df = exp_data_df[[type_col, args.column_name]]
        data_files_df = data_files_df.rename(columns={type_col: "data_file_type"})
        sampdesc_files_df = sampdesc_files_df.append(data_files_df)

    # Count the number of data file types by sample description.
    sampdesc_file_count = sampdesc_files_df.groupby([args.column_name, "data_file_type"])["data_file_type"].count()

    sampdesc_file_count.to_csv(sys.stdout, header=False)


if __name__ == "__main__":
    main()

# End of Program #
