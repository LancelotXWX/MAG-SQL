import sys
import json
import time
#from core.api_config import *
import openai
import os


# Your api settings
API_Key = "API Key"
Base_url = "The base url of your API"

#MODEL_NAME = 'gpt-4'
MODEL_NAME = 'gpt-3.5-turbo-16k'
#MODEL_NAME = 'gpt-4-0125-preview'
MAX_TRY = 5

# 用来传递外面的字典进来
world_dict = {}

log_path = None
api_trace_json_path = None
total_prompt_tokens = 0
total_response_tokens = 0


def init_log_path(my_log_path):
    global total_prompt_tokens
    global total_response_tokens
    global log_path
    global api_trace_json_path
    log_path = my_log_path
    total_prompt_tokens = 0
    total_response_tokens = 0
    dir_name = os.path.dirname(log_path)
    os.makedirs(dir_name, exist_ok=True)

    # 另外一个记录api调用的文件
    api_trace_json_path = os.path.join(dir_name, 'api_trace.json')


def api_func(prompt:str):
    global MODEL_NAME
    print(f"\nUse OpenAI model: {MODEL_NAME}\n")
    #client = openai.OpenAI(api_key="sk-hDrSXHyQSlCNVg7DC38966EfBe24492dAc02Ca541824D576",base_url="https://api3.apifans.com/v1")
    client = openai.OpenAI(api_key=API_Key,base_url=Base_url)
    
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        n = 1,
        temperature=0.1,
        max_tokens=1000
    )
    """
    response = client.completions.create(
        model=MODEL_NAME,
        prompt=prompt,
        n = 1,
        temperature=0.1,
        max_tokens=1000
    )
    """
    text = response.choices[0].message.content.strip()
    prompt_token = response.usage.prompt_tokens
    response_token = response.usage.completion_tokens
    
    """
    text= response.choices[0].text.strip()
    prompt_token = response.usage.prompt_tokens
    response_token = response.usage.completion_tokens
    """
    return text, prompt_token, response_token

def api_func_multiple_reply(prompt:str):
    global MODEL_NAME
    print(f"\nUse OpenAI model: {MODEL_NAME}\n")
    client = openai.OpenAI(api_key="sk-hDrSXHyQSlCNVg7DC38966EfBe24492dAc02Ca541824D576",base_url="https://api3.apifans.com/v1")
    #client = openai.OpenAI(api_key="sk-sTVtQR5V5VLxCQpOD909833cCb654b08Ac223c563eD33225",base_url="https://wei587.top/v1")
    
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        n = 2,
        temperature=0.2,
        max_tokens=1000
    )
    """
    response = client.completions.create(
        model=MODEL_NAME,
        prompt=prompt,
        n = 1,
        temperature=0.1,
        max_tokens=1000
    )
    """
    text_1 = response.choices[0].message.content.strip()
    text_2 = response.choices[1].message.content.strip()
    prompt_token = response.usage.prompt_tokens
    response_token = response.usage.completion_tokens
    
    """
    text= response.choices[0].text.strip()
    prompt_token = response.usage.prompt_tokens
    response_token = response.usage.completion_tokens
    """
    return [text_1,text_2], prompt_token, response_token



def safe_call_llm_multiple_reply(input_prompt, **kwargs) -> list:
    """
    函数功能描述：输入 input_prompt ，返回 模型生成的内容（内部自动错误重试5次，5次错误抛异常）
    """
    global MODEL_NAME
    global log_path
    global api_trace_json_path
    global total_prompt_tokens
    global total_response_tokens
    global world_dict

    for i in range(5):
        try:
            if log_path is None:
                # print(input_prompt)
                sys_response, prompt_token, response_token = api_func(input_prompt)
                print(f"\nsys_response: \n{sys_response}")
                print(f'\n prompt_token,response_token: {prompt_token} {response_token}\n')
            else:
                # check log_path and api_trace_json_path is not None
                if (log_path is None) or (api_trace_json_path is None):
                    raise FileExistsError('log_path or api_trace_json_path is None, init_log_path first!')
                with open(log_path, 'a+', encoding='utf8') as log_fp, open(api_trace_json_path, 'a+', encoding='utf8') as trace_json_fp:
                    print('\n' + f'*'*20 +'\n', file=log_fp)
                    print(input_prompt, file=log_fp)
                    print('\n' + f'='*20 +'\n', file=log_fp)
                    sys_response_list, prompt_token, response_token = api_func_multiple_reply(input_prompt)
                    print(sys_response_list, file=log_fp)
                    print(f'\n prompt_token,response_token: {prompt_token} {response_token}\n', file=log_fp)

                    if len(world_dict) > 0:
                        world_dict = {}
                    
                    if len(kwargs) > 0:
                        world_dict = {}
                        for k, v in kwargs.items():
                            world_dict[k] = v
                    # prompt response to world_dict
                    world_dict['response'] = '\n' + str(sys_response_list) + '\n'
                    world_dict['input_prompt'] = input_prompt.strip() + '\n'

                    world_dict['prompt_token'] = prompt_token
                    world_dict['response_token'] = response_token
                    

                    total_prompt_tokens += prompt_token
                    total_response_tokens += response_token

                    world_dict['cur_total_prompt_tokens'] = total_prompt_tokens
                    world_dict['cur_total_response_tokens'] = total_response_tokens

                    # world_dict to json str
                    world_json_str = json.dumps(world_dict, ensure_ascii=False)
                    print(world_json_str, file=trace_json_fp)

                    world_dict = {}
                    world_json_str = ''

                    print(f'\n total_prompt_tokens,total_response_tokens: {total_prompt_tokens} {total_response_tokens}\n', file=log_fp)
                    print(f'\n total_prompt_tokens,total_response_tokens: {total_prompt_tokens} {total_response_tokens}\n')
            return sys_response_list
        except Exception as ex:
            print(ex)
            print(f'Request {MODEL_NAME} failed. try {i} times. Sleep 10 secs.')
            time.sleep(10)

    raise ValueError('safe_call_llm error!')



def safe_call_llm(input_prompt, **kwargs) -> str:
    """
    函数功能描述：输入 input_prompt ，返回 模型生成的内容（内部自动错误重试5次，5次错误抛异常）
    """
    global MODEL_NAME
    global log_path
    global api_trace_json_path
    global total_prompt_tokens
    global total_response_tokens
    global world_dict

    for i in range(5):
        try:
            if log_path is None:
                # print(input_prompt)
                sys_response, prompt_token, response_token = api_func(input_prompt)
                print(f"\nsys_response: \n{sys_response}")
                print(f'\n prompt_token,response_token: {prompt_token} {response_token}\n')
            else:
                # check log_path and api_trace_json_path is not None
                if (log_path is None) or (api_trace_json_path is None):
                    raise FileExistsError('log_path or api_trace_json_path is None, init_log_path first!')
                with open(log_path, 'a+', encoding='utf8') as log_fp, open(api_trace_json_path, 'a+', encoding='utf8') as trace_json_fp:
                    print('\n' + f'*'*20 +'\n', file=log_fp)
                    print(input_prompt, file=log_fp)
                    print('\n' + f'='*20 +'\n', file=log_fp)
                    sys_response, prompt_token, response_token = api_func(input_prompt)
                    print(sys_response, file=log_fp)
                    print(f'\n prompt_token,response_token: {prompt_token} {response_token}\n', file=log_fp)

                    if len(world_dict) > 0:
                        world_dict = {}
                    
                    if len(kwargs) > 0:
                        world_dict = {}
                        for k, v in kwargs.items():
                            world_dict[k] = v
                    # prompt response to world_dict
                    world_dict['response'] = '\n' + sys_response.strip() + '\n'
                    world_dict['input_prompt'] = input_prompt.strip() + '\n'

                    world_dict['prompt_token'] = prompt_token
                    world_dict['response_token'] = response_token
                    

                    total_prompt_tokens += prompt_token
                    total_response_tokens += response_token

                    world_dict['cur_total_prompt_tokens'] = total_prompt_tokens
                    world_dict['cur_total_response_tokens'] = total_response_tokens

                    # world_dict to json str
                    world_json_str = json.dumps(world_dict, ensure_ascii=False)
                    print(world_json_str, file=trace_json_fp)

                    world_dict = {}
                    world_json_str = ''

                    print(f'\n total_prompt_tokens,total_response_tokens: {total_prompt_tokens} {total_response_tokens}\n', file=log_fp)
                    print(f'\n total_prompt_tokens,total_response_tokens: {total_prompt_tokens} {total_response_tokens}\n')
            return sys_response
        except Exception as ex:
            print(ex)
            print(f'Request {MODEL_NAME} failed. try {i} times. Sleep 10 secs.')
            time.sleep(10)

    raise ValueError('safe_call_llm error!')


if __name__ == "__main__":
    res = safe_call_llm('我爸妈结婚为什么不邀请我？')
    print(res)
