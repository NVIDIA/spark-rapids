# Copyright (c) 2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import subprocess

"""This script runs a number of instances of the TPC-DS dbsgen tool in parallel with each 
    instance generating one partition of data. The resulting files are then moved to a 
    directory structure with one directory per table so that the data can be read by 
    Apache Spark.
"""
def main():

    parser = argparse.ArgumentParser(description='Generate TPC-DS data.')
    parser.add_argument('--scale-factor', type=int, required=True,
                        help='Scale factor (number of GB To generate)')
    parser.add_argument('--partitions', type=int, required=True,
                        help='Number of partitions to generate in parallel')
    parser.add_argument('--dir', required=True,
                        help='Output directory')
    parser.add_argument('--dsdgen-dir', required=True,
                        help='Path to directory containing dsdgen binary')

    args = parser.parse_args()
    scale_factor = args.scale_factor
    num_partitions = args.partitions
    dsdgen_dir = args.dsdgen_dir
    output_dir = args.dir

    tables = [
        "call_center",
        "catalog_sales",
        "customer_demographics",
        "household_demographics",
        "item",
        "ship_mode",
        "store_sales",
        "web_page",
        "web_site",
        "catalog_page",
        "customer",
        "date_dim",
        "income_band",
        "promotion",
        "store",
        "time_dim",
        "web_returns",
        "catalog_returns",
        "customer_address",
        "inventory",
        "reason",
        "store_returns",
        "warehouse",
        "web_sales",
    ]

    # prepare directories
    os.system("mkdir {}".format(output_dir))
    for table in tables:
        os.system("mkdir {}/{}".format(output_dir, table))

    # generate data in parallel into separate directories
    proc = []
    for i in range(0, num_partitions):
        part_no = i+1
        part_dir = "{}/part-{}".format(output_dir, part_no)
        os.system("mkdir {}".format(part_dir))
        dsdgen_args = ["-SCALE", str(scale_factor), "-PARALLEL", str(num_partitions),
                       "-CHILD", str(part_no), "-DIR", str(part_dir)]
        proc.append(subprocess.Popen(["{}/dsdgen".format(dsdgen_dir)] + dsdgen_args,
                                     cwd = dsdgen_dir))

    # wait for data generation to complete
    for i in range(0, num_partitions):
        proc[i].wait()
        if proc[i].returncode != 0:
            print("dsdgen failed with return code {}".format(proc[i].returncode))
            raise Exception("dsdgen failed")

    # remove metadata file
    os.system("rm {}/part-1/dbgen_version* 2>/dev/null".format(output_dir))

    # rename and move files, for example:
    # mv /tmp/tpcds/part-1/catalog_page_1_2.dat /tmp/tpcds/catalog_page/
    for i in range(0, num_partitions):
        part_no = i+1
        part_dir = "{}/part-{}".format(output_dir, part_no)
        for table in tables:
            # not all partitions will contain files for all tables, so ignore errors here
            os.system("mv {}/{}_{}_{}.dat {}/{}/ 2>/dev/null".format(
                part_dir, table, part_no, num_partitions, output_dir, table))
        # remove temp partition directory - this will fail if any files still exist
        os.system("rmdir {}".format(part_dir))

if __name__ == '__main__':
    main()