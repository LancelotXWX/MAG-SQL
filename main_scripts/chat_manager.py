# -*- coding: utf-8 -*-
from main_scripts.MAG import Soft_Schema_linker, Decomposer, Generator, Refiner
from main_scripts.const import MAX_ROUND, SYSTEM_NAME, SCHEMALINKER_NAME, DECOMPOSER_NAME, REFINER_NAME

INIT_LOG__PATH_FUNC = None
LLM_API_FUC = None
try:
    from main_scripts import llm
    LLM_API_FUC = llm.safe_call_llm
    INIT_LOG__PATH_FUNC = llm.init_log_path
    print(f"Use func from main_scripts.llm in chat_manager.py")
except:
    print("Please prepare the code related to the large language model.")
import time
from pprint import pprint


class ChatManager(object):
    def __init__(self, data_path: str, tables_json_path: str, log_path: str, model_name: str, dataset_name:str, dataset_path:str, lazy: bool=False, without_selector: bool=False):
        self.data_path = data_path  # root path to database dir, including all databases
        self.tables_json_path = tables_json_path # path to table description json file
        self.log_path = log_path  # path to record important printed content during running
        self.model_name = model_name  # name of base LLM called by agent
        self.dataset_name = dataset_name
        self.ping_network()

        self.chat_group = [
            Soft_Schema_linker(data_path=self.data_path, tables_json_path=self.tables_json_path, model_name=self.model_name, dataset_name=dataset_name, dataset_path=dataset_path, lazy=lazy, without_selector=without_selector),
            Decomposer(),
            Generator(dataset_name=dataset_name),
            Refiner(data_path=self.data_path, dataset_name=dataset_name)
        ]
        INIT_LOG__PATH_FUNC(log_path)

    def ping_network(self):
        # check network status
        print("Checking network status...", flush=True)
        try:
            _ = LLM_API_FUC("Hello world!")
            print("Network is available", flush=True)
        except Exception as e:
            raise Exception(f"Network is not available: {e}")

    def _chat_single_round(self, message: dict):
        # we use `dict` type so value can be changed in the function
        for agent in self.chat_group:  # check each agent in the group
            if message['send_to'] == agent.name:
                agent.talk(message)

    def start(self, user_message: dict):
        # we use `dict` type so value can be changed in the function
        start_time = time.time()
        if user_message['send_to'] == SYSTEM_NAME:  # in the first round, pass message to prune
            #user_message['send_to'] = SELECTOR_NAME
            user_message['send_to'] = SCHEMALINKER_NAME
        for _ in range(MAX_ROUND):  # start chat in group
            self._chat_single_round(user_message)
            if user_message['send_to'] == SYSTEM_NAME:  # should terminate chat
                break
        end_time = time.time()
        exec_time = end_time - start_time
        print(f"\033[0;34mExecute {exec_time} seconds\033[0m", flush=True)


if __name__ == "__main__":
    test_manager = ChatManager(data_path="../data/spider/database",
                               log_path="",
                               model_name='gpt-35-turbo-instruct',
                               dataset_name='spider',
                               lazy=True)
    msg = {
        'db_id': 'concert_singer',
        'query': 'How many singers do we have?',
        'evidence': '',
        'extracted_schema': {},
        'ground_truth': 'SELECT count(*) FROM singer',
        'difficulty': 'easy',
        'send_to': SYSTEM_NAME
    }
    test_manager.start(msg)
    print(msg)
    print(msg['pred'])
