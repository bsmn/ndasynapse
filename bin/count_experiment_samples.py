#!/usr/bin/env python3

"""
Program: count_experiment_samples.py

Purpose: Count the number of unique values in the specified column for the given
         experiment ID.

Input parameters: Experiment ID
                  Optional Synapse ID of the sample file to be used for the
                      count. The default is syn18344730.
                  Optional column name to count. The default is
                      sample_id_original.

Outputs: Terminal output

Execution: count_experiment_samples.py experiment_id <experiment ID>
               --synapse_id <Synapse ID> --column_name <column name>
"""

import argparse
import pandas as pd
import synapseclient
import sys

# Synapse ID of the master sample list
MASTER_SAMPLE_SYN_ID = "syn18344730"

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("experiment_id", type=int,
                        help="Experiment ID for the sample count")
    parser.add_argument("--synapse_id", type=str, default=MASTER_SAMPLE_SYN_ID,
                        help="Synapse ID for the sample file")
    parser.add_argument("--column_name", type=str, default="sample_id_original",
                        help="Column to be counted")

    args = parser.parse_args()

    samp_syn = synapseclient.Synapse()
    samp_syn.login(silent=True)

    try:
        sample_file = samp_syn.get(args.synapse_id)
    except Exception as syn_id_error:
        raise syn_id_error

    sample_file_path = sample_file.path

    # The master sample file does not have the template name and version on the
    # first line. If the sample file being checked is not the master sample
    # file, skip the first row.
    if args.synapse_id == MASTER_SAMPLE_SYN_ID:
        sample_df = pd.read_csv(sample_file_path)
    else:
        sample_df = pd.read_csv(sample_file_path, skiprows=[0])

    # Make sure that the required columns are in the dataframe.
    if not ({'experiment_id', args.column_name}.issubset(sample_df.columns)):
        raise Exception(f"experiment_id and/or {args.column_name} is not a column in {args.synapse_id}")

    # Get only the records for the specified experiment ID
    sample_exp_df = sample_df.loc[sample_df['experiment_id'] == args.experiment_id]

    # Count the number of unique column values for the experiment and the total
    # number unique column values.
    samp_count = sample_exp_df.groupby(args.column_name)[args.column_name].nunique()
    samp_total_count = sample_exp_df[args.column_name].nunique()
    samp_total_series = samp_count.append(pd.Series({"Total": samp_total_count}))

    samp_total_series.to_csv(sys.stdout, header=False)

if __name__ == "__main__":
    main()
