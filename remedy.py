#!/usr/bin/python

# Copyright: (c) 2018, Terry Jones <terry.jones@example.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
from ansible.module_utils.basic import AnsibleModule
import filelock
import requests
import json
import os
from os import path
from time import sleep
import http.client
import ssl
import chardet
import mimetypes
import sys
import logging
from logging import getLogger, INFO
from concurrent_log_handler import ConcurrentRotatingFileHandler
from datetime import datetime

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.

__metaclass__ = type

DOCUMENTATION = r'''
---
module: remedy
short_description: Create Work Order with Remedy REST API

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.0"

description: This module creates a Work Order in the Remedy ITSM using BMC's REST API

options:
    name:
        description: This is the message to send to the test module.
        required: true
        type: str
    new:
        description:
            - Control to demo if the result of this module is changed or not.
            - Parameter description can be a list as well.
        required: false
        type: bool
# Specify this value according to your collection
# in format of namespace.collection.doc_fragment_name
extends_documentation_fragment:
    - my_namespace.my_collection.my_doc_fragment_name

author:
    - Your Name (@yourGitHubHandle)
'''

EXAMPLES = r'''
# Pass in a message
- name: Test with a message
  my_namespace.my_collection.my_test:
    name: hello world

# pass in a message and have changed true
- name: Test with a message and changed output
  my_namespace.my_collection.my_test:
    name: hello world
    new: true

# fail the module
- name: Test failure of the module
  my_namespace.my_collection.my_test:
    name: fail me
'''

RETURN = r'''
# These are examples of possible return values, and in general should use other names for return values.
original_message:
    description: The original name param that was passed in.
    type: str
    returned: always
    sample: 'hello world'
message:
    description: The output message that the test module generates.
    type: str
    returned: always
    sample: 'goodbye'
'''


# Module constants definitionwrite to syslog
CONST_NUMRETRIES = 3
CONST_TOKENFILE=""
CONST_LOGIN = '/api/jwt/login'
CONST_API = '/api/arsys/v1/entry'
CONST_LOGOUT = '/api/jwt/logout'
CONST_CREATEWO = '/WOI:WorkOrderInterface_Create'
CONST_MODIFY = '/WOI:WorkOrder'
CONST_ATTACHMENT = '/WOI:WorkInfo'
CONST_MESSAGE = ""
LOG = False
LOG_HANDLER = None
LOG_ID = None

def log(message):
  global LOG
  global LOG_HANDLER
  global LOG_ID
  if LOG:
    try:
        LOG_HANDLER.info(LOG_ID+": "+str(datetime.now())+": "+message)
    except Exception as e:
        pass

def logout(tokendir, apibase):
  try:
      tokenfile = CONST_TOKENFILE
      endpoint = apibase + CONST_LOGOUT
      log("Invalidating old token...")
      with open(tokenfile, 'r') as file:
          tokendata = file.read().replace('\n', '')
          file.close()
          hdrs = {'Authorization': 'AR-JWT ' + tokendata}
          response = requests.post(endpoint, headers=hdrs)
          log("Old token invalidated (status code: "+str(response.status_code)+")")
          return response
  except Exception as e:
      log("ERROR: "+str(e))
      response.status_code = 400
      return response

def login(tokendir, apibase, user, password):
    global CONST_MESSAGE
    log("Logging in (user: '"+user+"', url: '"+apibase+"'")
    try:
        tokenfile = CONST_TOKENFILE
        #q = {'username': user, 'password': password}
        q = [('username', user), ('password', password)]
        data = {}
        endpoint = apibase + CONST_LOGIN #+ "?username=" + user + "&password=" + password
        hdrs = {'Content-Type': 'application/x-www-form-urlencoded'}
        #response = requests.post(endpoint, data=data, params=q, headers=hdrs)
        response = requests.request("POST", endpoint, params=q, headers=hdrs, data=data)
        #response = requests.post(endpoint, data=data, headers=hdrs)
        if response.status_code == 200:
            log("Logged in successfully")
            tokendata = response.text
            with open(tokenfile, 'w+') as file:
                log("Writing token file...")
                file.write(tokendata)
                file.close()
            return response
        else:
            log("Could not login (status_code: "+str(response.status_code)+")")
            return response
    except Exception as e:
        log("Login error: "+str(e))
        CONST_MESSAGE += str(e)
        response.status_code = 400
        return response


def refreshtoken(tokendir, apibase, user, password):
    global CONST_MESSAGE
    log("Found invalid token. Refreshing...")
    lockfile = tokendir+"/token_refresh_"+user+".lock"
    if path.exists(lockfile):
      log("Token file locked. Waiting for lock to release...")
      sleep(3)
      return
    try:
        log("Token file locked. Trying to refresh...")
        open(lockfile, 'a+').close()
        lock = filelock.FileLock(lockfile, timeout=3)
        try:
          logout(tokendir, apibase)
        except Exception:
          pass
        response = login(tokendir, apibase, user, password)
        if response.status_code == 200:
            return True
        else:
            log("Token refresh error: Authentication failed.")
            CONST_MESSAGE += "Authentication Failed"
            return False
    except Exception as e:
        log("Token refresh error: "+str(e))
        CONST_MESSAGE += str(e)
        sleep(3)
        return False
    finally:
        log("Releasing token file lock...")
        lock.release(force=True)
        os.remove(lockfile)


def create(tokendir, apibase, data):
    global CONST_MESSAGE
    global CONST_TOKENFILE
    log("Creating WO...")
    try:
        tokenfile = CONST_TOKENFILE
        endpoint = apibase + CONST_API + CONST_CREATEWO
        q = {'fields': 'values(WorkOrder_ID)'}
        data=json.dumps(data)
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            hdrs = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            response = requests.post(endpoint, data=str(data), headers=hdrs, params=q, timeout=300)
            if response.status_code <= 204:
                log("WO Created (wiod: "+json.loads(response.text)["values"]["WorkOrder_ID"]+")")
                return response
            else:
                log("Could not create WO (status_code: "+str(response.status_code)+")")
                return response
    except Exception as e:
        log("WO Create ERROR: "+str(e))
        CONST_MESSAGE += str(e)
        response.status_code = 400
        return response


def getentryid(tokendir, apibase, woid):
    global CONST_MESSAGE
    try:
        tokenfile = CONST_TOKENFILE
        endpoint = apibase + CONST_API + CONST_MODIFY
        q = {'q': "'Work Order ID'"+"="+'"'+woid+'"'}
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            hdrs = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            response = requests.get(endpoint, params=q, headers=hdrs, timeout=180)
            if response.status_code == 200:
                return response
            else:
                response.status_code = 400
                return response
    except Exception as e:
        response.status_code = 400
        return response


def modify(tokendir, apibase, woid, data):
    global CONST_MESSAGE
    log("Modifying WO (woid: "+woid+") with status '"+data["values"]["Status"]+"'")
    try:
        tokenfile = CONST_TOKENFILE
        entryidresponse = getentryid(tokendir, apibase, woid)
        if entryidresponse.status_code == 400:
            return entryidresponse
        entryid = json.loads(entryidresponse.text)["entries"][0]["values"]["Request ID"]
        endpoint = apibase + CONST_API + CONST_MODIFY + "/" + entryid
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            hdrs = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            response = requests.put(endpoint, json=data, headers=hdrs)
            if response.status_code == 204:
                log("WO modified successfully (woid: "+woid+")")
                return response
            else:
                log("Could not modify WO (woid: "+woid+")")
                return response
    except Exception as e:
        log("WO modify error: "+str(e))
        CONST_MESSAGE += str(e)
        response.status_code = 400
        return response


def addattachment(tokendir, apibase, woid, data, filename):
    global CONST_MESSAGE
    log("Adding attachment (woid: "+woid+")")
    try:
        head, tail = os.path.split(filename)
        entryidresponse = getentryid(tokendir, apibase, woid)
        if entryidresponse.status_code == 400:
            return entryidresponse
        entryid = json.loads(entryidresponse.text)["entries"][0]["values"]["Request ID"]
        apibase = apibase.replace('https://', '')
        conn = http.client.HTTPSConnection(apibase, 443, context=ssl._create_unverified_context())
        tokenfile = CONST_TOKENFILE
        data["values"]["Work Order ID"] = woid
        data["values"]["WorkOrder_EntryID"] = woid
        data["values"]["z2AF Work Log01"] = tail
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            dataList = []
            boundary = b'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
            dataList.append(b'--' + boundary)
            dataList.append(b'Content-Disposition: form-data; name="entry";')
            dataList.append(b'Content-Type: application/json')
            dataList.append(b'')
            dataList.append(json.dumps(data).encode())
            dataList.append(b'--' + boundary)
            dataList.append(b'Content-Disposition: form-data; name="attach-z2AF Work Log01"; filename='+tail.encode())
            fileType = b'application/octet-stream'
            dataList.append(b'Content-Type: '+fileType)
            dataList.append(b'')
            with open(filename, 'rb') as f:
                filecontent = f.read()
                encoding = chardet.detect(filecontent)
                dataList.append(filecontent)
                f.close()
            dataList.append(b'--' + boundary + b'--')
            dataList.append(b'')
            body = b'\r\n'.join(dataList)
            payload = body
            headers = {
                b'Authorization': b'AR-JWT ' + tokendata.encode(),
                b'Accept-Encoding': b'gzip, deflate, br',
                b'Content-type': b'multipart/form-data; boundary=' + boundary
            }
            conn.request("POST",  CONST_API + CONST_ATTACHMENT, payload, headers)
            res = conn.getresponse()
            log("File attached successfully (woid: "+woid+")")
            return res
    except Exception as e:
        log("File attachment error: "+str(e))
        CONST_MESSAGE += str(e)
        return 400


def run_module():
    global CONST_TOKENFILE
    global CONST_MESSAGE
    global LOG_HANDLER
    global LOG
    global LOG_ID
    # define available arguments/parameters a user can pass to the module
    module_args = dict(
        token_dir=dict(type='str', required=True),
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        apibase=dict(type='str', required=True),
        action=dict(type='str', choices=['create', 'modify', 'add_attachment'], required=True),
        data=dict(type='dict', required=True),
        woid=dict(type='str', required=False),
        filename=dict(type='str', required=False),
        logfile=dict(type="str", required=False, default="None"),
        log=dict(type="bool", required=False, default=False),
        log_identifier=dict(type="str", required=False, default=""),
    )

    # seed the result dict in the object
    # we primarily care about changed and state
    # changed is if this module effectively modified the target
    # state will include any data that you want your module to pass back
    # for consumption, for example, in a subsequent task
    result = dict(
        changed=False,
        original_message='',
        message=''
    )

    # the AnsibleModule object will be our abstraction working with Ansible
    # this includes instantiation, a couple of common attr would be the
    # args/params passed to the execution, as well as if the module
    # supports check mode
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False
    )

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)
    fname = module.params["token_dir"] + "/token_"+module.params['user']+".txt"
    if not os.path.exists(fname):
        with open(fname, 'a') as f:
            f.write("")
            f.close()
        refreshtoken(module.params["token_dir"], module.params["apibase"], module.params["user"], module.params["password"])
    CONST_TOKENFILE = fname
    if module.params["log"]:
        if module.params["logfile"] == "None":
          CONST_MESSAGE += "Logging file not specified. Logging NOT enabled"
          LOG=False
        else:
            try:
                LOG_HANDLER = getLogger(__name__)
                # Use an absolute path to prevent file rotation trouble.
                logfile = os.path.abspath(module.params["logfile"])
                # Rotate log after reaching 100M, keep 5 old copies.
                rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 100*1024*1024, 3)
                LOG_HANDLER.addHandler(rotateHandler)
                LOG_HANDLER.setLevel(INFO)
                LOG = True
                LOG_ID = module.params["log_identifier"]
            except Exception as e:
                CONST_MESSAGE += "Could not create logfile. Logging NOT enabled: "+str(e)
                LOG = False
    


    # manipulate or modify the state as
    # needed (this is going to be the
    # part where your module will do what it needs to do)
    result['original_message'] = module.params['action']
    #module.params["data"] = str(module.params["data"].encode("ascii","replace"))
    # use whatever logic you need to determine whether or not this module
    # made any modifications to your target
    if module.params['action'] == 'create':
        for i in range(CONST_NUMRETRIES):
            try:
                response = create(module.params["token_dir"], module.params["apibase"], module.params["data"])
                result['message'] = response.text
                if response.status_code <= 204:
                    result["message"] = json.loads(response.text)["values"]["WorkOrder_ID"]
                    result['changed'] = True
                    break
                elif response.status_code >= 400:
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'])
            except Exception as e:
                result['message']=str(e)
        if not result['changed']:
            result['message'] += CONST_MESSAGE
            module.fail_json(msg='ERROR: Could not create Work Order', **result)
        else:
            module.exit_json(**result)

    elif module.params['action'] == 'add_attachment':
        for i in range(CONST_NUMRETRIES):
            try:
                response = addattachment(module.params["token_dir"], module.params["apibase"], module.params["woid"], module.params["data"], module.params["filename"])
                if response.status == 201:
                    result['changed'] = True
                    result['message'] = response.read()
                    break
                elif response.status >= 400:
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'])
            except:
                refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'])
        if not result['changed']:
            result['message'] += CONST_MESSAGE
            module.fail_json(msg='ERROR: Could not attach file to Work Order', **result)
        else:
            module.exit_json(**result)

    elif module.params['action'] == 'modify':
        for i in range(CONST_NUMRETRIES):
            try:
                response = modify(module.params["token_dir"], module.params["apibase"], module.params["woid"], module.params["data"])
                if response.status_code == 204:
                    result['changed'] = True
                    result['message'] = response.text
                    break
                elif response.status_code >= 400:
                    result['message'] = response.text
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'])
            except Exception as e:
                result['message'] = str(e)
        if not result['changed']:
            result['message'] += CONST_MESSAGE
            module.fail_json(msg='ERROR: Could not modify Work Order', **result)
        else:
            module.exit_json(**result)

def main():
    run_module()


if __name__ == '__main__':
    main()
