#!/usr/bin/python3

import requests
import json
import sys

# TODO Externalize headers and WHITELISTED ACCOUNTS

vtex_api_from_account_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}
vtex_api_to_account_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}
vtex_api_from_account_cookies = {}
vtex_api_to_account_cookies = {}

destination_whitelist = ["ssesandbox03","ssesandbox04","bravtexpetstore","bravtexeletrostore","bravtexgrocerystore","bravtexfashionb2b"]

def read_api_as_JSON(cookies, headers, url, params=""):
    if (params != ""):
        print('params='+json.dumps(params))
    try:
        response = requests.request("GET", url, headers=headers, cookies=cookies, params=params)
        if (response.status_code == 200):
            try:
                return json.loads(response.text)
            except ValueError:
                return False
        else:
            return False
    except requests.exceptions.RequestException as e:
        print(f'read_api_as_JSON REQUEST ERROR: {e}')
        return False

def write_JSON_to_api(cookies, headers, url, payload=""):
    """ write payload data via POST to url
    :param cookies: must contain VtexIdclientAutCookie key and value
    :param url: url to send POST to
    :param payload: payload to send
    :return: returns Tuple with (response.text as JSON,status_code,response_text) or (False,status_code,response_text)
    """
    is_safe_to_continue=False
    for whitelisted_destination in destination_whitelist:
        if (url.find(whitelisted_destination) != -1): is_safe_to_continue=True

    if is_safe_to_continue==False:
        print(f"Sorry, destination url '{url}' does not contain a WHITELISTED account! Please check. Program ABORTED!")
        sys.exit()
    
    # print('[INFO] WRITE URL='+url)
    # print('[INFO] payload='+json.dumps(payload))
    try:
        response = requests.request("POST", url, json=payload,headers=headers, cookies=cookies)
        if (response.status_code == 200):
            try:
                return (json.loads(response.text),200,response.text)
            except ValueError:
                return (True,200,"")
        else:
            # print('[ERROR] WRITE URL='+url)
            # print("[ERROR] Status Code="+str(response.status_code))
            # print("[ERROR] Result="+response.text)
            return (False,response.status_code,response.text)
    except requests.exceptions.RequestException as e:
        print(f'write_JSON_to_api REQUEST ERROR: {e}')
        return(False,500,'')

def send_DELETE_to_api(cookies, headers, url, payload=""):
    """ send DELETE verb and payload to url
    :param cookies: must contain VtexIdclientAutCookie key and value
    :param url: url to send DELETE to
    :param payload: any payload to send
    :return: returns Tuple with (response.text as JSON,status_code,response_text) or (False,status_code,response_text)
    """
    is_safe_to_continue=False
    for whitelisted_destination in destination_whitelist:
        if (url.find(whitelisted_destination) != -1): is_safe_to_continue=True

    if is_safe_to_continue==False:
        print(f"Sorry, destination url '{url}' does not contain a WHITELISTED account! Please check. Program ABORTED!")
        sys.exit()

    # print('[INFO] DELETE URL='+url)
    # print('[INFO] payload='+json.dumps(payload))
    try:
        response = requests.request("DELETE", url, json=payload,headers=headers, cookies=cookies)
        if (response.status_code == 200):
            try:
                return (json.loads(response.text),response.status_code,response.text)
            except:
                return (True, response.status_code, response.text)
        else:
            # print('[ERROR] DELETE URL='+url)
            # print("[ERROR] Status Code="+str(response.status_code))
            # print("[ERROR] Result="+response.text)
            return (False, response.status_code, response.text)
    except requests.exceptions.RequestException as e:
        print(f'send_DELETE_to_api REQUEST ERROR: {e}')
        return(False,500,'')

def update_JSON_to_api(cookies, headers, url, payload=""):
    """ write payload data via PUT to url
    :param cookies: must contain VtexIdclientAutCookie key and value
    :param url: url to send PUT to
    :param payload: payload to send
    :return: returns Tuple with (response.text as JSON,status_code,response_text) or (False,status_code,response_text)
    """
    is_safe_to_continue=False
    for whitelisted_destination in destination_whitelist:
        if (url.find(whitelisted_destination) != -1): is_safe_to_continue=True

    if is_safe_to_continue==False:
        print(f"Sorry, destination url '{url}' does not contain a WHITELISTED account! Please check. Program ABORTED!")
        sys.exit()
    
    # print('[INFO] WRITE URL='+url)
    # print('[INFO] payload='+json.dumps(payload))
    try:
        response = requests.request("PUT", url, json=payload,headers=headers, cookies=cookies)
        if (response.status_code == 200):
            if response.text:
                return (json.loads(response.text),200,response.text)
            else:
                return ("{}",200,"(empty response)")
        else:
            # print('[ERROR] WRITE URL='+url)
            # print("[ERROR] Status Code="+str(response.status_code))
            # print("[ERROR] Result="+response.text)
            return (False,response.status_code,response.text)
    except requests.exceptions.RequestException as e:
        print(f'update_JSON_to_api REQUEST ERROR: {e}')
        return(False,500,'')

# Print iterations progress
# Based on https://stackoverflow.com/questions/3173320/text-progress-bar-in-terminal-with-block-characters
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()