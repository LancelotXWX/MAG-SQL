# -*- coding: utf-8 -*-
import pandas as pd
import json
from func_timeout import func_timeout,FunctionTimedOut
from main_scripts.utils import parse_json, check_letter, contain_value, add_prefix, load_json_file, extract_world_info, is_email, is_valid_date_column, extract_sql, detect_special_char, add_quotation_mark, get_matched_content_sequence, get_chosen_schema, extract_subquery
from main_scripts.bridge_content_encoder import get_matched_entries
LLM_API_FUC = None

# try import core.api, if error then import core.llm
try:
    from main_scripts import llm
    LLM_API_FUC = llm.safe_call_llm
    print(f"Use func from core.llm in agents.py")
except:
    print("Please prepare the code related to the large language model.")

from main_scripts.const import *
from typing import List
from copy import deepcopy

import sqlite3
import time
import abc
import sys
import os
import pandas as pd
from tqdm import tqdm, trange


class BaseAgent(metaclass=abc.ABCMeta):
    # Define a base class 
    def __init__(self):
        pass

    @abc.abstractmethod
    def talk(self, message: dict):
        pass


class Soft_Schema_linker(BaseAgent):
    """
    Get database description and then extract entities from questions before selecting related schema
    """
    name = SCHEMALINKER_NAME
    description = "Get database description and then extract entities from questions before selecting related schema"

    def __init__(self, data_path: str, tables_json_path: str, model_name: str, dataset_name:str, dataset_path:str, lazy: bool = False, without_selector: bool = False):
        super().__init__()
        self.data_path = data_path.strip('/').strip('\\')
        self.tables_json_path = tables_json_path
        self.model_name = model_name
        self.dataset_name = dataset_name
        self.dataset_path = dataset_path
        # summary of db (stay in the memory during generating prompt)
        self.db2infos = {}  
        # store all db to tables.json dict by tables_json_path
        self.db2dbjsons = {}
        self.init_db2jsons()
        if not lazy:
            self._load_all_db_info()
        self.total_content_dict = {}
        # match the related values in database
        self.match_dict = self._data_prematch()
        self._message = {}
        # summarize each table
        self.db_summary = self._get_summary()
        self.without_selector = without_selector
    
    def init_db2jsons(self):
        if not os.path.exists(self.tables_json_path):
            raise FileNotFoundError(f"tables.json not found in {self.tables_json_path}")
        data = load_json_file(self.tables_json_path)
        for item in data:
            db_id = item['db_id']
            
            table_names = item['table_names']
            # Count the tables
            item['table_count'] = len(table_names)
            
            column_count_lst = [0] * len(table_names)
            for tb_idx, col in item['column_names']:
                if tb_idx >= 0:
                    column_count_lst[tb_idx] += 1
            # Three different indicators on the number of columns
            item['max_column_count'] = max(column_count_lst)
            item['total_column_count'] = sum(column_count_lst)
            item['avg_column_count'] = sum(column_count_lst) // len(table_names)
            
            # print()
            # print(f"db_id: {db_id}")
            # print(f"table_count: {item['table_count']}")
            # print(f"max_column_count: {item['max_column_count']}")
            # print(f"total_column_count: {item['total_column_count']}")
            # print(f"avg_column_count: {item['avg_column_count']}")
            time.sleep(0.2)
            self.db2dbjsons[db_id] = item
    
    
    def _get_column_attributes(self, cursor, table):
        # Query specific information about this table in the database
        cursor.execute(f"PRAGMA table_info(`{table}`)")
        columns = cursor.fetchall()

        # Construct a list where each element is a dictionary describing the attributes of a column
        columns_info = []
        primary_keys = []
        column_names = []
        column_types = []
        for column in columns:
            column_names.append(column[1])
            column_types.append(column[2])
            is_pk = bool(column[5])
            if is_pk:
                primary_keys.append(column[1])
            column_info = {
                'name': column[1],  # The name of this column
                'type': column[2],  # Data type
                'not_null': bool(column[3]),  # Whether the value can be null or not
                'primary_key': bool(column[5])  # Is it the primary key
            }
            columns_info.append(column_info)
            # since no other information is used later , the full information (the whole dictionary) is not returned, just the two lists
        return column_names, column_types

    
    def _get_unique_column_values_str(self, cursor, table, column_names, column_types, 
                                      json_column_names, is_key_column_lst):

        col_to_values_str_lst = []
        col_to_values_str_dict = {}

        key_col_list = [json_column_names[i] for i, flag in enumerate(is_key_column_lst) if flag]

        len_column_names = len(column_names)

        for idx, column_name in enumerate(column_names):
            # Get the distinct value of each column, selects the value of the specified column from the specified table, and groups the columns by value. Then sort each group in descending order by the number of records in the group.
            # print(f"In _get_unique_column_values_str, processing column: {idx}/{len_column_names} col_name: {column_name} of table: {table}", flush=True)

            # skip pk and fk
            if column_name in key_col_list:
                continue
            
            lower_column_name: str = column_name.lower()
            # if lower_column_name ends with [id, email, url], just use empty str
            if lower_column_name.endswith('id') or \
                lower_column_name.endswith('email') or \
                lower_column_name.endswith('url'):
                values_str = ''
                col_to_values_str_dict[column_name] = values_str
                continue

            sql = f"SELECT `{column_name}` FROM `{table}` GROUP BY `{column_name}` ORDER BY COUNT(*) DESC"
            cursor.execute(sql)
            values = cursor.fetchall()
            values = [value[0] for value in values]

            values_str = ''
            # try to get value examples str, if exception, just use empty str
            try:
                values_str = self._get_value_examples_str(values, column_types[idx])
            except Exception as e:
                print(f"\nerror: get_value_examples_str failed, Exception:\n{e}\n")

            col_to_values_str_dict[column_name] = values_str


        for k, column_name in enumerate(json_column_names):
            values_str = ''
            # print(f"column_name: {column_name}")
            # print(f"col_to_values_str_dict: {col_to_values_str_dict}")

            is_key = is_key_column_lst[k]

            # pk or fk do not need value str
            if is_key:
                values_str = ''
            elif column_name in col_to_values_str_dict:
                values_str = col_to_values_str_dict[column_name]
            else:
                print(col_to_values_str_dict)
                time.sleep(3)
                print(f"error: column_name: {column_name} not found in col_to_values_str_dict")
            
            col_to_values_str_lst.append([column_name, values_str])
        
        return col_to_values_str_lst
    

    def _get_value_examples_str(self, values: List[object], col_type: str):
        # Get the values stored in a column and refine them according to the column's specific situation
        if not values:
            return ''
        if len(values) > 10 and col_type in ['INTEGER', 'REAL', 'NUMERIC', 'FLOAT', 'INT']:
            return ''
        
        vals = []
        has_null = False
        for v in values:
            if v is None:
                has_null = True
            else:
                tmp_v = str(v).strip()
                if tmp_v == '':
                    continue
                else:
                    vals.append(v)
        if not vals:
            return ''
        
        # drop meaningless values
        if col_type in ['TEXT', 'VARCHAR']:
            new_values = []
            
            for v in vals:
                if not isinstance(v, str):
                    
                    new_values.append(v)
                else:
                    if self.dataset_name == 'spider':
                        v = v.strip()
                    if v == '': # exclude empty string
                        continue
                    elif ('https://' in v) or ('http://' in v): # exclude url
                        return ''
                    elif is_email(v): # exclude email
                        return ''
                    else:
                        new_values.append(v)
            vals = new_values
            tmp_vals = [len(str(a)) for a in vals]
            if not tmp_vals:
                return ''
            max_len = max(tmp_vals)
            if max_len > 50:
                return ''
        
        if not vals:
            return ''
        
        vals = vals[:6]

        is_date_column = is_valid_date_column(vals)
        if is_date_column:
            vals = vals[:1]

        if has_null:
            vals.insert(0, None)
        
        val_str = str(vals)
        return val_str
    
    def _load_single_db_info(self, db_id: str) -> dict:
        table2coldescription = {} # Dict {table_name: [(column_name, full_column_name, column_description), ...]}
        table2primary_keys = {} # DIct {table_name: [primary_key_column_name,...]}
        
        table_foreign_keys = {} # Dict {table_name: [(from_col, to_table, to_col), ...]}
        table_unique_column_values = {} # Dict {table_name: [(column_name, examples_values_str)]}

        db_dict = self.db2dbjsons[db_id]

        # todo: gather all pk and fk id list
        important_key_id_lst = []
        keys = db_dict['primary_keys'] + db_dict['foreign_keys']
        for col_id in keys:
            if isinstance(col_id, list):
                important_key_id_lst.extend(col_id)
            else:
                important_key_id_lst.append(col_id)


        db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")  # avoid gbk/utf8 error, copied from sql-eval.exec_eval
        cursor = conn.cursor()

        table_names_original_lst = db_dict['table_names_original']
        for tb_idx, tb_name in enumerate(table_names_original_lst):
            # Iterate over the original column names in this list
            all_column_names_original_lst = db_dict['column_names_original']
            all_column_names_full_lst = db_dict['column_names']
            col2dec_lst = []

            pure_column_names_original_lst = []
            is_key_column_lst = []
            # column_names, column_types
            all_sqlite_column_names_lst, all_sqlite_column_types_lst = self._get_column_attributes(cursor, tb_name)
            col_coltype_dict = dict(zip(all_sqlite_column_names_lst,all_sqlite_column_types_lst))
            for col_idx, (root_tb_idx, orig_col_name) in enumerate(all_column_names_original_lst):
                if root_tb_idx != tb_idx:
                    continue
                pure_column_names_original_lst.append(orig_col_name)
                if col_idx in important_key_id_lst:
                    is_key_column_lst.append(True)
                else:
                    is_key_column_lst.append(False)
                full_col_name: str = all_column_names_full_lst[col_idx][1]
                full_col_name = full_col_name.replace('_', ' ')
                col_type = col_coltype_dict[orig_col_name]
                cur_desc_obj = [orig_col_name, full_col_name, col_type, '']
                col2dec_lst.append(cur_desc_obj)
            table2coldescription[tb_name] = col2dec_lst
            
            table_foreign_keys[tb_name] = []
            table_unique_column_values[tb_name] = []
            table2primary_keys[tb_name] = []

            # column_names, column_types
            all_sqlite_column_names_lst, all_sqlite_column_types_lst = self._get_column_attributes(cursor, tb_name)
            col_to_values_str_lst = self._get_unique_column_values_str(cursor, tb_name, all_sqlite_column_names_lst, all_sqlite_column_types_lst, pure_column_names_original_lst, is_key_column_lst)
            table_unique_column_values[tb_name] = col_to_values_str_lst
        
        # table_foreign_keys
        foreign_keys_lst = db_dict['foreign_keys']

        for from_col_idx, to_col_idx in foreign_keys_lst:
            from_col_name = all_column_names_original_lst[from_col_idx][1]
            from_tb_idx = all_column_names_original_lst[from_col_idx][0]
            from_tb_name = table_names_original_lst[from_tb_idx]

            to_col_name = all_column_names_original_lst[to_col_idx][1]
            to_tb_idx = all_column_names_original_lst[to_col_idx][0]
            to_tb_name = table_names_original_lst[to_tb_idx]

            table_foreign_keys[from_tb_name].append((from_col_name, to_tb_name, to_col_name))
        

        # table2primary_keys
        for pk_idx in db_dict['primary_keys']:
            # if pk_idx is int
            pk_idx_lst = []
            if isinstance(pk_idx, int):
                pk_idx_lst.append(pk_idx)
            elif isinstance(pk_idx, list):
                pk_idx_lst = pk_idx
            else:
                err_message = f"pk_idx: {pk_idx} is not int or list"
                print(err_message)
                raise Exception(err_message)
            for cur_pk_idx in pk_idx_lst:
                tb_idx = all_column_names_original_lst[cur_pk_idx][0]
                col_name = all_column_names_original_lst[cur_pk_idx][1]
                tb_name = table_names_original_lst[tb_idx]
                table2primary_keys[tb_name].append(col_name)
        
        cursor.close()
        # print table_name and primary keys
        # for tb_name, pk_keys in table2primary_keys.items():
        #     print(f"table_name: {tb_name}; primary key: {pk_keys}")
        time.sleep(3)

        # wrap result and return
        result = {
            "desc_dict": table2coldescription,
            "value_dict": table_unique_column_values,
            "pk_dict": table2primary_keys,
            "fk_dict": table_foreign_keys
        }
        return result

    def _load_all_db_info(self):
        print("\nLoading all database info...", file=sys.stdout, flush=True)
        db_ids = [item for item in os.listdir(self.data_path)]
        for i in trange(len(db_ids)):
            db_id = db_ids[i]
            db_info = self._load_single_db_info(db_id)
            self.db2infos[db_id] = db_info
    
    def _data_prematch(self):
        # Value matching based on LCS algorithm
        matched_dict_path = './data/bird/dev/match_content.json'
        if os.path.exists(matched_dict_path):
            with open(matched_dict_path, 'r', encoding='utf-8') as f:
                print(f"load json file from {matched_dict_path}")
                match_dict = json.load(f)
                return match_dict
        else:
            dataset = load_json_file(self.dataset_path)
            # coarse-grained matching between the input text and all contents in database
            match_dict = {}
            print("Get matched values from database: ")
            for data in tqdm(dataset):
                db_id = data["db_id"]
                question_id = data["question_id"]
                db_dict = self.db2dbjsons[db_id]
                db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
                conn = sqlite3.connect(db_path)
                conn.text_factory = lambda b: b.decode(errors="ignore")  # avoid gbk/utf8 error, copied from sql-eval.exec_eval
                cursor = conn.cursor()
                table_names_original_lst = db_dict['table_names_original']
                if self.total_content_dict.get(db_id) == None:
                    self.total_content_dict[db_id] = {}
                    for tb_idx, tb_name in enumerate(table_names_original_lst):
                        self.total_content_dict[db_id][tb_name] = {}
                        # Iterate over the original column names in this list
                        all_column_names_original_lst = db_dict['column_names_original']
                        all_sqlite_column_names_lst, all_sqlite_column_types_lst = self._get_column_attributes(cursor, tb_name)
                        col_coltype_dict = dict(zip(all_sqlite_column_names_lst,all_sqlite_column_types_lst))
                        for col_idx, (root_tb_idx, orig_col_name) in enumerate(all_column_names_original_lst):
                            if orig_col_name != '*':
                                if root_tb_idx != tb_idx:
                                    continue
                                col_type = col_coltype_dict[orig_col_name]
                                if col_type != "TEXT":
                                    continue
                                sql = f"SELECT `{orig_col_name}` FROM `{tb_name}` GROUP BY `{orig_col_name}` ORDER BY COUNT(*) DESC"
                                cursor.execute(sql)
                                values = cursor.fetchall()
                                values = [value[0] for value in values]
                                check = False
                                for value in values:
                                    if value != None:
                                        if not value.isdecimal():
                                            check = True
                                if not check:
                                    continue
                                self.total_content_dict[db_id][tb_name][orig_col_name] = values
                inputs = data['evidence'] + ". " + data['question']
                matched_list = []
                for tb_name, contents_dict in self.total_content_dict[db_id].items():
                    for col_name,contents in contents_dict.items():
                        if len(contents) > 2000:
                            continue
                        fm_contents = get_matched_entries(inputs, contents)
                        if fm_contents is None:
                            continue
                        for _match_str, (field_value, _s_match_str, match_score, s_match_score, _match_size,) in fm_contents:
                            if match_score < 0.9:
                                continue
                            if field_value.isdecimal() or len(field_value) == 1:
                                continue
                            matched_list.append(f"{tb_name}.`{col_name}` = \'" + field_value + "\'")
                matched_list = list(set(matched_list))
                matched_list_top10 = sorted(matched_list,key = lambda i:len(i),reverse=True)[:10] 
                match_dict[question_id] = matched_list_top10
            json_match_dict = json.dumps(match_dict,indent=4)
            with open(matched_dict_path, 'w') as f:
                f.write(json_match_dict)
            return match_dict
    
    
    def _load_descripition(self, db_id, table_name):
        # If there is a description of each column, save it.
        try:
            db_path = f"{self.data_path}/{db_id}/database_description/{table_name}.csv"
            col_desc_dict = pd.read_csv(db_path)
            col_name_list = list(col_desc_dict['original_column_name'])
            for i in range(len(col_name_list)):
                col_name_list[i] = col_name_list[i].strip()
            col_desc = list(col_desc_dict['column_description'].fillna(''))
            col_desc_dict = dict(zip(col_name_list,col_desc))
            return col_desc_dict
        except:
            return {}
        
        
    def _build_bird_table_schema_sqlite_str(self, table_name, new_columns_desc, new_columns_val):
        schema_desc_str = ''
        schema_desc_str += f"CREATE TABLE {table_name}\n"
        extracted_column_infos = []
        for (col_name, full_col_name, col_type, col_extra_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
            # district_id INTEGER PRIMARY KEY, -- location of branch
            col_line_text = ''
            col_extra_desc = 'And ' + str(col_extra_desc) if col_extra_desc != '' and str(col_extra_desc) != 'nan' else ''
            col_extra_desc = col_extra_desc[:100]
            col_line_text = ''
            col_line_text += f"  {col_name},  --"
            if full_col_name != '':
                full_col_name = full_col_name.strip()
                col_line_text += f" {full_col_name},"
            if col_values_str != '':
                col_line_text += f" Value examples: {col_values_str}."
            if col_extra_desc != '':
                col_line_text += f" {col_extra_desc}"
            extracted_column_infos.append(col_line_text)
        schema_desc_str += '{\n' + '\n'.join(extracted_column_infos) + '\n}' + '\n'
        return schema_desc_str
    
    def _build_part_bird_table_schema_list_str(self, table_name, new_columns_desc, new_columns_val, db_id):
        # Schema Representation without description and value examples
        schema_desc_str = ''
        schema_desc_str += f"# {table_name}: "
        extracted_column_infos = []

        for (col_name, full_col_name, col_type, col_extra_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
            col_line_text = ''
            col_line_text += f"{col_name} ("
            col_line_text += f"{col_type}), "
            extracted_column_infos.append(col_line_text)
        schema_desc_str += '[' + ', '.join(extracted_column_infos).strip(',') + ']' + '\n'
        
        return schema_desc_str
    
    def _build_total_bird_table_schema_list_str(self, table_name, new_columns_desc, new_columns_val, db_id):
        # Complete Schema Representation
        descrip_dict = self._load_descripition(table_name=table_name,db_id=db_id)
        schema_desc_str = ''
        schema_desc_str += f"# Table: {table_name}\n"
        extracted_column_infos = []
        for (col_name, full_col_name, col_type, col_extra_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
            col_extra_desc = 'And ' + str(col_extra_desc) if col_extra_desc != '' and str(col_extra_desc) != 'nan' else ''
            col_extra_desc = col_extra_desc[:100]

            col_line_text = ''
            col_line_text += f'  ('
            col_line_text += f"{col_name} <{col_type}>, "
            if full_col_name != '':
                full_col_name = full_col_name.strip()
                if descrip_dict != {}:
                    desc_col = descrip_dict[col_name].strip()
                    if len(desc_col) > len(full_col_name):
                        col_line_text += f" {desc_col}." 
                    else:
                        col_line_text += f" {full_col_name}."
                else:
                    col_line_text += f" {full_col_name}."
            if col_values_str != '':
                col_line_text += f" Value examples: {col_values_str}."
            if col_extra_desc != '':
                col_line_text += f" {col_extra_desc}"
            col_line_text += '),'
            extracted_column_infos.append(col_line_text)
        schema_desc_str += '[\n' + '\n'.join(extracted_column_infos).strip(',') + '\n]' + '\n'
        return schema_desc_str
        
    def _get_related_details(self, table_name, new_columns_desc, new_columns_val, extracted_schema, db_id):
        # Add the appropriate description and value examples to each column of data
        descrip_dict = self._load_descripition(table_name=table_name,db_id=db_id)
        related_details = ''
        llm_chosen_columns = extracted_schema.get(table_name,[])
        for i in range(len(llm_chosen_columns)):
            llm_chosen_columns[i] = llm_chosen_columns[i].strip('`')
        for (col_name, full_col_name, col_type, col_extra_desc), (_, col_values_str) in zip(new_columns_desc, new_columns_val):
            if col_name in llm_chosen_columns:
                col_details = ''
                col_details += f"{table_name}.`{col_name}`: "
                if full_col_name != '':
                    full_col_name = full_col_name.strip()
                    if descrip_dict != {}:
                        desc_col = descrip_dict[col_name].strip()
                        if len(desc_col) > len(full_col_name):
                            col_details += f"The column \'{col_name}\' in Table <{table_name}> has column descriptions of \"{desc_col}\". "
                        else:
                            col_details += f"The column \'{col_name}\' in Table <{table_name}> has column descriptions of \"{full_col_name}\". "
                    else:
                        col_details += f"The column \'{col_name}\' in Table <{table_name}> has column descriptions of \"{full_col_name}\". "
                if col_values_str != '':
                    col_details += f" Value examples: {col_values_str}."
                if col_extra_desc != '':
                    col_details += f" {col_extra_desc}"
                col_details += "\n"
                related_details += col_details
        return related_details
    
    def _get_db_desc_str(self,
                         db_id: str,
                         extracted_schema: dict,
                         matched_content_dict: dict = None,
                         use_gold_schema: bool = False,
                         complete: bool = True) -> List[str]:
        """
        Add foreign keys, and value descriptions of focused columns.
        :param db_id: name of sqlite database
        :param extracted_schema: {table_name: "keep_all" or "drop_all" or ['col_a', 'col_b']}
        :return: Detailed columns info of db; foreign keys info of db
        """
        if self.db2infos.get(db_id, {}) == {}:  # lazy load
            self.db2infos[db_id] = self._load_single_db_info(db_id)
        db_info = self.db2infos[db_id]
        desc_info = db_info['desc_dict']  # table:str -> columns[(column_name, full_column_name, extra_column_desc): str]
        value_info = db_info['value_dict']  # table:str -> columns[(column_name, value_examples_str): str]
        pk_info = db_info['pk_dict']  # table:str -> primary keys[column_name: str]
        fk_info = db_info['fk_dict']  # table:str -> foreign keys[(column_name, to_table, to_column): str]
        tables_1, tables_2, tables_3 = desc_info.keys(), value_info.keys(), fk_info.keys()
        assert set(tables_1) == set(tables_2)
        assert set(tables_2) == set(tables_3)

        # print(f"desc_info: {desc_info}\n\n")

        # schema_desc_str = f"[db_id]: {db_id}\n"
        schema_desc_str = ''  # for concat
        related_details_str = '' # also for concat
        db_fk_infos = []  # use list type for unique check in db
        db_pk_infos = [] # use list type for primary keys
        filtered_matched_content_dict = {}
        # print(f"extracted_schema:\n")
        # pprint(extracted_schema)
        # print()

        print(f"db_id: {db_id}")
        # For selector recall and compression rate calculation
        chosen_db_schem_dict = {} # {table_name: ['col_a', 'col_b'], ..}

        for (table_name, columns_desc), (_, columns_val), (_, fk_info), (_, pk_info) in \
                zip(desc_info.items(), value_info.items(), fk_info.items(), pk_info.items()):
            
            new_columns_desc = deepcopy(columns_desc)
            new_columns_val = deepcopy(columns_val)
            
            # Filtered tables
            chosen_db_schem_dict[table_name] = [col_name for col_name, _, _, _ in new_columns_desc]
            # Only provides matches for the columns that pass the filter
            if matched_content_dict != None:
                for col_name, _, _ in new_columns_desc:
                    correponding_column = table_name + '.' + col_name
                    if correponding_column.lower() in matched_content_dict.keys():
                        filtered_matched_content_dict[correponding_column] = matched_content_dict[correponding_column.lower()]
            else:
                filtered_matched_content_dict = None
            # 1. Build schema part of prompt
            # schema_desc_str += self._build_bird_table_schema_sqlite_str(table_name, new_columns_desc, new_columns_val)
            if complete:
                schema_desc_str += self._build_total_bird_table_schema_list_str(table_name, new_columns_desc, new_columns_val, db_id)
            else:
                schema_desc_str += self._build_part_bird_table_schema_list_str(table_name, new_columns_desc, new_columns_val, db_id)
            related_details_str += self._get_related_details(table_name, new_columns_desc, new_columns_val, extracted_schema, db_id)
            # 2. Build foreign key part of prompt
            for col_name, to_table, to_col in fk_info:
                from_table = table_name
                if '`' not in str(col_name):
                    col_name = f"`{col_name}`"
                if '`' not in str(to_col):
                    to_col = f"`{to_col}`"
                fk_link_str = f"{from_table}.{col_name} = {to_table}.{to_col}"
                if fk_link_str not in db_fk_infos:
                    db_fk_infos.append(fk_link_str)
            if len(pk_info) > 0: 
                pk_str = f"{table_name}.`{pk_info[0]}`"
            else:
                pk_str = ""
            db_pk_infos.append(pk_str)

        fk_desc_str = '\n'.join(db_fk_infos)
        pk_desc_str = ' | '.join(db_pk_infos)
        schema_desc_str = schema_desc_str.strip()
        fk_desc_str = fk_desc_str.strip()
        pk_desc_str = pk_desc_str.strip()
        match_desc_str = get_matched_content_sequence(filtered_matched_content_dict)
        return schema_desc_str, fk_desc_str, pk_desc_str, chosen_db_schem_dict, match_desc_str, related_details_str

    def _get_summary(self):
        # Summarize each table
        dataset = load_json_file(self.dataset_path)
        db_ids = set([data["db_id"] for data in dataset])
        db_schema_dict = {}
        db_summary = {}
        for db_id in db_ids:
            db_schema, db_fk, db_pk, chosen_db_schem_dict, match_content, column_details = self._get_db_desc_str(db_id=db_id, extracted_schema={}, use_gold_schema=False, complete=True)
            db_schema_dict[db_id] = db_schema
        for key,value in db_schema_dict.items():
            prompt = summarizer_template.format(db_id=key,desc_str=value)
            summary_json = LLM_API_FUC(prompt,**db_schema_dict)
            summary_dict = parse_json(summary_json)
            temp_str = ''
            for tb_name,tb_summary in summary_dict.items():
                temp_str += f"# {tb_name}: {tb_summary}\n"
            db_summary[key] = temp_str.strip()
        print(db_summary)
        return db_summary
            
    def _is_need_prune(self, db_id: str, db_schema: str):
        # We can judge whether to filter by the number of columns and the number of tables, or by the number of tokens obtained after the database schema in the prompt has been encoded
        """
        encoder = tiktoken.get_encoding("cl100k_base")
        tokens = encoder.encode(db_schema)
        return len(tokens) >= 25000
        """
        db_dict = self.db2dbjsons[db_id]
        avg_column_count = db_dict['avg_column_count']
        total_column_count = db_dict['total_column_count']
        if avg_column_count <= 5 and total_column_count <= 25:
            return False
        else:
            return True

    def _prune(self,
               db_id: str,
               query: str,
               db_schema: str,
               db_pk: str,
               db_fk: str,
               evidence: str = None,
               matched_list: list = []) -> dict:

        if matched_list != []:
            matched_str = '; '.join(matched_list)
        else:
            matched_str = 'No matched values.'
        #prompt = schema_linker_template.format(db_id=db_id, query=query, evidence=evidence, desc_str=db_schema, fk_str=db_fk, pk_str=db_pk, matched_list=matched_str)
        prompt = schema_linker_template.format(db_id=db_id, query=query, evidence=evidence, desc_str=db_schema, fk_str=db_fk, pk_str=db_pk, matched_list=matched_str, summary_str=self.db_summary[db_id])
        word_info = extract_world_info(self._message)
        print(prompt)
        reply = LLM_API_FUC(prompt, **word_info)
        print(reply)
        extracted_schema_dict = get_chosen_schema(parse_json(reply))
        return extracted_schema_dict

    def talk(self, message: dict):
        """
        :param message: {"db_id": database_name,
                         "query": user_query,
                         "evidence": extra_info,
                         "extracted_schema": None if no preprocessed result found},
                         "matched_contents": the matched values from 
        :return: extracted database schema {"desc_str": extracted_db_schema, "fk_str": foreign_keys_of_db}
        """
        if message['send_to'] != self.name: return
        self._message = message
        idx, db_id, ext_sch, query, evidence = message.get('idx'), \
                                            message.get('db_id'), \
                                            message.get('extracted_schema', {}), \
                                            message.get('query'), \
                                            message.get('evidence'), \

        use_gold_schema = False
        if ext_sch:
            use_gold_schema = True

        print(type(idx))
        if self.match_dict.get(0) == None:
            idx = str(idx)
        else:
            idx = int(idx)

        matched_list = self.match_dict.get(idx)
        message['matched_list'] = matched_list
        print(matched_list)
        db_schema, db_fk, db_pk, chosen_db_schem_dict, match_content, column_details = self._get_db_desc_str(db_id=db_id, extracted_schema=ext_sch, use_gold_schema=use_gold_schema, complete=True)
        message['complete_desc_str'] = db_schema
        message['summary_str'] = self.db_summary[db_id].strip()
        need_prune = self._is_need_prune(db_id, db_schema)
        if self.without_selector:
            need_prune = False
        if ext_sch == {} and need_prune:
            raw_extracted_schema_dict = self._prune(db_id=db_id, query=query, db_schema=db_schema, db_fk=db_fk, db_pk=db_pk, evidence=evidence, matched_list=matched_list)
            print(f"query: {message['query']}\n")
            db_schema, db_fk, db_pk, chosen_db_schem_dict, match_content, column_details = self._get_db_desc_str(db_id=db_id, extracted_schema=raw_extracted_schema_dict, use_gold_schema=use_gold_schema, complete=False)
            
            message['extracted_schema'] = raw_extracted_schema_dict
            message['chosen_db_schem_dict'] = chosen_db_schem_dict
            message['desc_str'] = db_schema
            message['fk_str'] = db_fk
            message['pk_str'] = db_pk
            message['pruned'] = True
            message['match_content_str'] = match_content
            message['columns_details_str'] = column_details
            print(column_details)
            message['send_to'] = DECOMPOSER_NAME
        else:
            message['chosen_db_schem_dict'] = chosen_db_schem_dict
            message['desc_str'] = db_schema
            message['fk_str'] = db_fk
            message['pk_str'] = db_pk
            message['pruned'] = False
            message['match_content_str'] = match_content
            message['send_to'] = DECOMPOSER_NAME

class Decomposer(BaseAgent):
    """
    Decompose the question into Targets and Conditions, and then splice them one by one to get a series of Sub-questions
    """
    name = DECOMPOSER_NAME
    description = "Decompose the question into Targets and Conditions, and then splice them one by one to get a series of Sub-questions"

    def __init__(self):
        super().__init__()
        self._message = {}
        # self.corellm = ChatModel() # This Agent can use a fine-tuned LLM

    def talk(self, message: dict):
        """
        :param self:
        :param message: {"query": user_query,
                        "evidence": extra_info,
                        "desc_str": description of db schema,
                        "fk_str": foreign keys of database}
        :return: decompose question into sub ones and solve them in generated SQL
        """
        if message['send_to'] != self.name: return
        self._message = message
        query, evidence = message.get('query'), \
                          message.get('evidence')
        prompt = pure_decomposer_template.format(query=query, evidence=evidence)
        word_info = extract_world_info(self._message)
        reply = LLM_API_FUC(prompt, **word_info)
        print(reply)
        reply_list = extract_subquery(reply)
        if reply_list == []:
            reply_list.append(query)
        message['subquery_list'] = reply_list
        # Increase fault tolerance by replacing the last sub-question with the original question
        message['subquery_list'][-1] = query
        message['initial_state'] = True
        message['send_to'] = GENERATOR_NAME
        
    
class Generator(BaseAgent):
    """
    Generate Sub-SQL iteratively using CoT
    """
    name = GENERATOR_NAME
    description = "Generate Sub-SQL iteratively using CoT"

    def __init__(self, dataset_name):
        super().__init__()
        self.dataset_name = dataset_name
        self._message = {}
        # self.corellm = ChatModel() # This Agent can use a fine-tuned LLM

    def talk(self, message: dict):
        """
        :param self:
        :param message: {"query": user_query,
                        "evidence": extra_info,
                        "desc_str": description of db schema,
                        "fk_str": foreign keys of database}
        :return: decompose question into sub ones and solve them in generated SQL
        """
        if message['send_to'] != self.name: return
        self._message = message
        evidence, schema_info, fk_info, matched_list, subqueries = message.get('evidence'), \
                                                                    message.get('desc_str'), \
                                                                    message.get('fk_str'), \
                                                                    message.get('matched_list'), \
                                                                    message.get('subquery_list')
        pk_info = message.get('pk_str')
        column_details = message.get('columns_details_str')
        if matched_list != []:
            matched_str = '; '.join(matched_list)
        else:
            matched_str = 'No matched values.'
        focus_query = subqueries[0]
        initial = message['initial_state']
        # check if the current subquery is the first subquery
        if initial:
            prompt = soft_schema_initial_generator_template.format(query=focus_query, evidence=evidence, desc_str=schema_info, fk_str=fk_info, pk_str=pk_info, detailed_str=column_details, matched_list=matched_str)
            message['initial_state'] = False
        else:
            subquery = message['last_subquery']
            subsql = message['sub_sql']
            prompt = soft_schema_continuous_generator_template.format(query=focus_query, evidence=evidence, desc_str=schema_info, fk_str=fk_info, pk_str=pk_info, detailed_str=column_details, subquery=subquery, subsql=subsql, matched_list=matched_str)
        print(prompt)
        word_info = extract_world_info(self._message)
        reply = LLM_API_FUC(prompt,**word_info)
        print(reply)
        sql_statement = extract_sql(reply)
        message['old_chain_of_thoughts'] = reply
        message['final_sql'] = sql_statement
        message['fixed'] = False
        message['send_to'] = REFINER_NAME
              
class Refiner(BaseAgent):
    name = REFINER_NAME
    description = "Execute SQL and preform validation"

    def __init__(self, data_path: str, dataset_name: str):
        super().__init__()
        self.data_path = data_path  # path to all databases
        self.dataset_name = dataset_name
        self._message = {}

    def run_sql(self,sql: str, db_id: str):
        db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return result

    def _execute_sql(self, sql: str, db_id: str) -> dict:
        # Get database connection
        """
        db_path = f"{self.data_path}/{db_id}/{db_id}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="ignore")
        cursor = conn.cursor()
        """
        try:
            result = func_timeout(30,self.run_sql,args=(sql,db_id))
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            """
            return {
                "sql": str(sql),
                "data": result[:5],
                "sqlite_error": "",
                "exception_class": ""
            }
        except FunctionTimedOut as te:
            return {
                "sql": str(sql),
                "sqlite_error": str(te.args),
                "exception_class": str(type(te).__name__)
            }
        except sqlite3.Error as er:
            return {
                "sql": str(sql),
                "sqlite_error": str(' '.join(er.args)),
                "exception_class": str(er.__class__)
            }
        except Exception as e:
            return {
                "sql": str(sql),
                "sqlite_error": str(e.args),
                "exception_class": str(type(e).__name__)
            }

    @staticmethod
    def _is_need_refine(exec_result: dict, try_times: int):
        data = exec_result.get('data', None)
        if try_times >= 2:
            return False
        if data is not None:
            if len(data) == 0:
                exec_result['sqlite_error'] = 'no data selected'
                return True
            if len(data) == 1:
                return False
            check_all_None = True
            check_None = False
            for t in data:
                for n in t:
                    if n is None:  
                        exec_result['sqlite_error'] = 'exist None value, you can add `IS NOT NULL` in SQL'
                        check_None = True
                    else:
                        check_all_None = False
            if check_None and not check_all_None:
                # There are NULL values but not all values are null
                return True
            return False
        else:
            return True

    def _value_retriver(self, target: str, db_id: str, db_content_dict: dict, related_schema: dict):
        inputs = target.strip('\'').strip('%')
        matched_list = []
        for tb_name, contents_dict in db_content_dict.items():
            related_cols = related_schema.get(tb_name,[])
            for col_name,contents in contents_dict.items():
                if col_name in related_cols:
                    matched_contents = []
                    for v in contents:
                        if v != None:
                            if inputs.lower().replace(" ","") in v.lower().replace(" ","") and len(v) <= 2 + len(inputs) and v != inputs:
                                matched_contents.append(v)
                    for match_str in matched_contents:
                        matched_list.append(f"{tb_name}.`{col_name}` = \'" + match_str + "\'")
        matched_list = list(set(matched_list))
        matched_list_top5 = sorted(matched_list,key = lambda i:len(i),reverse=False)[:4] 
        return matched_list_top5
    
    def _judge_value(self, sql: str):
        values_targets = []
        value_list = contain_value(sql)
        if value_list != None:
            for item in value_list:
                if check_letter(item):
                    values_targets.append(item)
        return values_targets
        
    
    def _refine(self,
               db_id: str,
               query: str,
               evidence:str,
               schema_info: str,
               pk_info: str,
               fk_info: str,
               column_details: str,
               error_info: dict,
               complete_schema: str,
               matched_content: str) -> dict:
        
        sql_arg = add_prefix(error_info.get('sql'))
        sqlite_error = error_info.get('sqlite_error')
        exception_class = error_info.get('exception_class')
        if "no such column" in sqlite_error.lower():
            sqlite_error = sqlite_error + " (Check if the column in the SQL is selected from the correct table based on the 【Database info】 at first, and then check if the column name is enclosed in backticks.)"
            """
            prompt = refiner_template.format(query=query, evidence=evidence, desc_str=complete_schema, \
                                       fk_str=fk_info, sql=sql_arg, sqlite_error=sqlite_error, \
                                        exception_class=exception_class)
            """
            prompt = refiner_template.format(query=query, evidence=evidence, desc_str=complete_schema, \
                                        pk_str=pk_info, detailed_str=column_details, fk_str=fk_info, \
                                        sql=sql_arg, sqlite_error=sqlite_error, \
                                        exception_class=exception_class)
            filter_error = True
        elif "no data selected" in sqlite_error.lower() and sql_arg.count('SELECT') > 1:
            prompt = nested_refiner_template.format(query=query, evidence=evidence, desc_str=schema_info, \
                                        pk_str=pk_info, detailed_str=column_details, fk_str=fk_info, \
                                        sql=sql_arg, sqlite_error=sqlite_error, matched_content=matched_content, \
                                        exception_class=exception_class)
            print(prompt)
            filter_error = False
        else:
            prompt = refiner_template.format(query=query, evidence=evidence, desc_str=schema_info, \
                                        pk_str=pk_info, detailed_str=column_details, fk_str=fk_info, \
                                        sql=sql_arg, sqlite_error=sqlite_error, \
                                        exception_class=exception_class)
            filter_error = False

        word_info = extract_world_info(self._message)
        reply = LLM_API_FUC(prompt, **word_info)
        print(reply)
        res = extract_sql(reply)
        return res, filter_error 

    def talk(self, message: dict):
        """
        Execute SQL and preform validation
        :param message: {"query": user_query,
                        "evidence": extra_info,
                        "desc_str": description of db schema,
                        "fk_str": foreign keys of database,
                        "final_sql": generated SQL to be verified,
                        "db_id": database name to execute on}
        :return: execution result and if need, refine SQL according to error info
        """
        if message['send_to'] != self.name: return
        self._message = message
        db_id, old_sql, query, evidence, schema_info, fk_info, complete_schema = message.get('db_id'), \
                                                                                message.get('pred', message.get('final_sql')), \
                                                                                message.get('subquery_list')[0], \
                                                                                message.get('evidence'), \
                                                                                message.get('desc_str'), \
                                                                                message.get('fk_str'), \
                                                                                message.get('complete_desc_str')
        pk_info = message.get('pk_str')
        column_details = message.get('columns_details_str')
        if message.get('matched_list') != []: 
            matched_content = '; '.join(message.get('matched_list'))
        else:
            matched_content = 'No matched values.'
        error_info = self._execute_sql(old_sql, db_id)
        try_times =  message.get('try_times', 0)
        need_refine = self._is_need_refine(error_info, try_times)
        if not need_refine:  # correct in one pass or refine success
            if ' || \' \' || ' in old_sql:
                old_sql = old_sql.replace(' || \' \' || ',', ')
            old_sql = old_sql.replace('ASC LIMIT','ASC NULLS LAST LIMIT')
            print("Final predicted sql: ",old_sql)
            message['try_times'] = message.get('try_times', 0) + 1
            message['pred'] = old_sql
            if len(message['subquery_list']) == 1:
                message['send_to'] = SYSTEM_NAME
            else:
                message['last_subquery'] = message['subquery_list'][0]
                message['subquery_list'].pop(0)
                message['sub_sql'] = old_sql
                message['try_times'] = 0
                res = message.pop('pred')
                message['send_to'] = GENERATOR_NAME
            
        else:
            #new_sql, filter_error = self._refine(query, evidence, schema_info, fk_info, error_info, complete_schema)
            new_sql, filter_error = self._refine(db_id, query, evidence, schema_info, pk_info, fk_info, column_details, error_info, complete_schema, matched_content)
            if filter_error:
                message['desc_str'] = message['complete_desc_str']
            message['try_times'] = message.get('try_times', 0) + 1
            message['pred'] = new_sql
            message['fixed'] = True
            message['send_to'] = REFINER_NAME
        return 

if __name__ == "__main__":
    m = 0