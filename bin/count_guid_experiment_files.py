#!/usr/bin/env python3

"""
Program: count_guid_experiment_files.py

Purpose: Uses the output sample file from manifest_guid_data.py to break down 
         the totals of each file type for the given experiment.

Input parameters: Input file
                  Experiment ID

Outputs: stdout

Execution: manifest_guid_data.py <NDA credentials file> 
               <NDA manifest type> <output file>
               --synapse_id <Synapse ID> --column_name <column name>
"""

import argparse
import collections
import pandas as pd

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("guid_input_file", type=argparse.FileType("r"),
                        help="Sample file created by manifest_guid_data.py")
    parser.add_argument("experiment_id", type=int,
                        help="Experiment ID")

    args = parser.parse_args()

    file_type_list = []

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
        file_type_list += exp_data_df[type_col].tolist()

    # Not every sample has four files, so get rid of any blank values in the list.
    file_type_list = [file_type for file_type in file_type_list if str(file_type) != "nan"]

    file_counter = collections.Counter(file_type_list)

    # Convert the Counter object to a dictionary so the output is prettier.
    file_counter_dict = dict(file_counter)

    print(f"Experiment {args.experiment_id}: {file_counter_dict}")


if __name__ == "__main__":
    main()

# End of Program #
