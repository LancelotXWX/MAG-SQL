#!/bin/bash

python ./run.py --dataset_name bird \
   --dataset_mode dev \
   --input_file ./data/bird/dev/dev.json \
   --db_path ./data/bird/dev/dev_databases/ \
   --tables_json_path ./data/bird/dev/dev_tables.json \
   --output_file ./output/gpt4_mag.json \
   --log_file ./output/log/log/txt \
   --start_pos 0 

echo "Generate SQL on bird dev data done!"


