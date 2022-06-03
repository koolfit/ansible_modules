#!/usr/bin/python
#
#------------------------------- HOW TO USE
# Este play esta diseñado para crear una Work Order en Remedy por medio de su API
# y enviar estadisticas de uso de las automatizaciones a Grafana.
#
#------------------------------- REQUERIMIENTOS
#
#
#
#------------------------------- ENTRADAS
#
#
#
#------------------------------- EJEMPLO DE USO
#
#
#
#------------------------------- MAIN

# Copyright: (c) 2018, Terry Jones <terry.jones@example.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
from email.policy import default
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
import traceback

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
CONST_TOKENFILE = ''
CONST_LOGIN = '/api/jwt/login'
CONST_API = '/api/arsys/v1/entry'
CONST_LOGOUT = '/api/jwt/logout'
CONST_CREATEWO = '/WOI:WorkOrderInterface_Create'
CONST_MODIFY = '/WOI:WorkOrder'
CONST_ATTACHMENT = '/WOI:WorkInfo'
CONST_TABLA_PRODUCTO_COMPANIA = '/PCT:ProductCompanyAssocLookup'
CONST_TABLA_OPERACIONES_COMPANIA = '/CFG:Service Catalog LookUp'
CONST_TABLA_COMPANIA_SUPPORT_GRUPS = '/CTM:Support Group'
CONST_TABLA_ASIGNACION = '/KIO:CFG:Assignment'
CONST_TABLA_USUARIOS= '/CTM:People'
CONST_MESSAGE = ''
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

def logout(apibase, hdrs):
    try:
        tokenfile = CONST_TOKENFILE
        endpoint = apibase + CONST_LOGOUT
        log("Invalidating old token...")
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            file.close()
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata}
            hdrs = dict(hdr_token, **hdrs)
            response = requests.post(endpoint, headers=hdrs)
            log("Old token invalidated (status code: "+str(response.status_code)+")")
            return response
    except Exception as e:
        log("ERROR: "+str(e))
        response.status_code = 400
        return response

def login(apibase, user, password, hdrs):
    global CONST_MESSAGE
    log("Logging in (user: '"+user+"', url: '"+apibase+"'")
    try:
        tokenfile = CONST_TOKENFILE
        q = [('username', user), ('password', password)]
        data = {}
        endpoint = apibase + CONST_LOGIN
        hdr_token = {'Content-Type': 'application/x-www-form-urlencoded'}
        hdrs = dict(hdr_token, **hdrs)
        response = requests.request("POST", endpoint, params=q, headers=hdrs, data=data)
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

def refreshtoken(tokendir, apibase, user, password, hdrs):
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
          logout(apibase, hdrs)
        except Exception:
          pass
        response = login(apibase, user, password, hdrs)
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

def create(apibase, data, hdrs):
    global CONST_MESSAGE
    global CONST_TOKENFILE

    # log("Validating minimal data...")
    # try:
    #     pass
    # except Exception as e:
    #     pass

    log("Creating WO...")
    try:
        tokenfile = CONST_TOKENFILE
        endpoint = apibase + CONST_API + CONST_CREATEWO
        q = {'fields': 'values(WorkOrder_ID)'}
        data=json.dumps(data)
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            hdrs = dict(hdr_token, **hdrs)
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

def getentryid(apibase, woid, hdrs):
    global CONST_MESSAGE
    try:
        tokenfile = CONST_TOKENFILE
        endpoint = apibase + CONST_API + CONST_MODIFY
        q = {'q': "'Work Order ID'"+"="+'"'+woid+'"'}
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            hdrs = dict(hdr_token, **hdrs)
            response = requests.get(endpoint, params=q, headers=hdrs, timeout=180)
            if response.status_code == 200:
                return response
            else:
                response.status_code = 400
                return response
    except Exception as e:
        response.status_code = 400
        return response

def get_generic_user(apibase, company, hdrs):
    global CONST_MESSAGE
    tokenfile = CONST_TOKENFILE
    try:
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')

            endpoint = apibase + CONST_API + CONST_TABLA_USUARIOS
            q = {'q': "'Company'"+"="+'"'+company+'"'}
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            hdrs = dict(hdr_token, **hdrs)

            response_users = requests.get(endpoint, params=q, headers=hdrs, timeout=180)
            if response_users.status_code >= 400:
                return response_users
            

            users = json.loads(response_users.text)["entries"]
            usuario_completo = list(filter(lambda usuario: usuario if 'USUARIO' in usuario['values']['Full Name'] else None, users))[0]

            usuario = {
                        'Person ID':   usuario_completo['values']['Person ID'],
                        'Full Name':   usuario_completo['values']['Full Name'],
                        'First Name':  usuario_completo['values']['First Name'],
                        'Last Name':   usuario_completo['values']['Last Name'],
                        'status_code': 200
                        }
            return usuario

    except Exception as e:
        log("Get Categories error: "+str(e))
        CONST_MESSAGE += str(e)
        response_users.status_code = 400
        return response_users

def get_support_group_name(apibase, suport_group_id, hdrs):
    global CONST_MESSAGE
    tokenfile = CONST_TOKENFILE
    try:
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')

            endpoint = apibase + CONST_API + CONST_TABLA_COMPANIA_SUPPORT_GRUPS
            q = {'q': "'Support Group ID'"+"="+'"'+suport_group_id+'"'}
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            hdrs = dict(hdr_token, **hdrs)
            response_support_group = requests.get(endpoint, params=q, headers=hdrs, timeout=180)
            
            if response_support_group.status_code >= 400:
                return response_support_group
            

            support_group_completo = json.loads(response_support_group.text)["entries"][0]
            

            support_group = {
                        'Support Organization':   support_group_completo['values']['Support Organization'],
                        'Support Group Name':   support_group_completo['values']['Support Group Name'],
                        'status_code': 200
                        }
            return support_group

    except Exception as e:
        log("Get Categories error: "+str(e))
        CONST_MESSAGE += str(e)
        response_support_group.status_code = 400
        return response_support_group

#para recibir el grupo no la tecnologia
#def get_categories(apibase, company, assigned_group, defautl_technology):
def get_categories(apibase, company, technology, defautl_technology, hdrs):
    global CONST_MESSAGE
    tokenfile = CONST_TOKENFILE

    def extract_values(assigment):
        filtered_assigments = {
                            'Submitter':                assigment['values']['Submitter'],
                            'chr_EmpresaResolutora__c': assigment['values']['chr_EmpresaResolutora__c'],
                            'Support Organization__c':  assigment['values']['Support Organization__c'],
                            'Categorization Tier 1__c': assigment['values']['Categorization Tier 1__c'],
                            'Categorization Tier 2__c': assigment['values']['Categorization Tier 2__c'],
                            'Categorization Tier 3__c': assigment['values']['Categorization Tier 3__c'],
                            'Support Group ID__c':      assigment['values']['Support Group ID__c'],
                            'Assigned Group__c':        assigment['values']['Assigned Group__c'],
                            'Support Company__c':       assigment['values']['Support Company__c']
                            }
        return filtered_assigments

    try:
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')

            endpoint = apibase + CONST_API + CONST_TABLA_ASIGNACION
            q = {'q': "'Contact Company__c'"+"="+'"'+company+'"'}
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            hdrs = dict(hdr_token, **hdrs)
            response_assignments = requests.get(endpoint, params=q, headers=hdrs, timeout=180)
            
            if response_assignments.status_code >= 400:
                return response_assignments

            #Obteniendo usuario generico
            usuario = get_generic_user(apibase, company)

            if usuario['status_code'] != 200 or ( type(usuario) is not dict and usuario.status_code >= 400):
                return usuario

            #Revisando Asignaciones
            assignments = json.loads(response_assignments.text)["entries"]
            assigments_ordered = {'default': [], 'other_values': []}

            default = None
            tec = None

            for assigment_full in assignments:
                
                #Filtrando datos para asgnar WO
                assigment = extract_values(assigment_full)

                #Agregando datos del usuario
                assigment['Person ID']  = usuario['Person ID']
                assigment['Full Name']  = usuario['Full Name']
                assigment['First Name'] = usuario['First Name']
                assigment['Last Name']  = usuario['Last Name']

                #para recibir el grupo no la tecnologia
                #if assigned_group in assigment['Assigned Group__c']:
                if technology in assigment['Categorization Tier 3__c']:
                    tec = assigment

                if defautl_technology in assigment['Categorization Tier 3__c']:
                    default = assigment

                assigments_ordered['other_values'].append(assigment)

            if tec is None:
                assigments_ordered['default'] = default
            else:
                assigments_ordered['default'] = tec

            assigments_ordered['status_code'] = 200

            return assigments_ordered

    except Exception as e:
        log("Get Categories error: "+str(e))
        CONST_MESSAGE += str(e)
        response_assignments.status_code = 400
        return response_assignments

def modify(apibase, woid, data, hdrs):
    global CONST_MESSAGE
    log("Modifying WO (woid: "+woid+") with status '"+data["values"]["Status"]+"'")
    try:
        tokenfile = CONST_TOKENFILE
        entryidresponse = getentryid(apibase, woid, hdrs)
        if entryidresponse.status_code == 400:
            return entryidresponse
        entryid = json.loads(entryidresponse.text)["entries"][0]["values"]["Request ID"]
        endpoint = apibase + CONST_API + CONST_MODIFY + "/" + entryid
        with open(tokenfile, 'r') as file:
            tokendata = file.read().replace('\n', '')
            hdr_token = {'Authorization': 'AR-JWT ' + tokendata, 'Content-Type': 'application/json'}
            hdrs = dict(hdr_token, **hdrs)
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

def addattachment(apibase, woid, data, filename, hdrs):
    global CONST_MESSAGE
    log("Adding attachment (woid: "+woid+")")
    try:
        head, tail = os.path.split(filename)
        entryidresponse = getentryid(apibase, woid, hdrs)
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
            hdr_token = {
                b'Authorization': b'AR-JWT ' + tokendata.encode(),
                b'Accept-Encoding': b'gzip, deflate, br',
                b'Content-type': b'multipart/form-data; boundary=' + boundary
            }
            hdr_token = {b'Accept-Encoding': b'gzip, deflate, br', b'Content-type': b'multipart/form-data; boundary='}
            hdrs = dict(hdr_token, **hdrs)
            conn.request("POST",  CONST_API + CONST_ATTACHMENT, payload, hdrs)
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
        action=dict(type='str', choices=['create', 'modify', 'add_attachment', 'get_categories'], required=True),
        data=dict(type='dict', required=True),
        headers=dict(type='dict', required=False, default={}),
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
    fname = module.params['token_dir'] + "/token_"+module.params['user']+".txt"
    if not os.path.exists(fname):
        with open(fname, 'a') as f:
            f.write("")
            f.close()
        refreshtoken(module.params['token_dir'], module.params["apibase"], module.params["user"], module.params["password"], module.params["headers"])
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
                response = create(module.params["apibase"], module.params["data"], module.params["headers"])
                result['message'] = response.text
                if response.status_code <= 204:
                    result["message"] = json.loads(response.text)["values"]["WorkOrder_ID"]
                    result['changed'] = True
                    break
                elif response.status_code >= 400:
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'], module.params["headers"])
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
                response = addattachment(module.params["apibase"], module.params["woid"], module.params["data"], module.params["filename"], module.params["headers"])
                if response.status == 201:
                    result['changed'] = True
                    result['message'] = response.read()
                    break
                elif response.status >= 400:
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'], module.params["headers"])
            except:
                refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'], module.params["headers"])
        if not result['changed']:
            result['message'] += CONST_MESSAGE
            module.fail_json(msg='ERROR: Could not attach file to Work Order', **result)
        else:
            module.exit_json(**result)

    elif module.params['action'] == 'modify':
        for i in range(CONST_NUMRETRIES):
            try:
                response = modify(module.params["apibase"], module.params["woid"], module.params["data"], module.params["headers"])
                if response.status_code == 204:
                    result['changed'] = True
                    result['message'] = response.text
                    break
                elif response.status_code >= 400:
                    result['message'] = response.text
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'], module.params["headers"])
            except Exception as e:
                result['message'] = str(e)
        if not result['changed']:
            result['message'] += CONST_MESSAGE
            module.fail_json(msg='ERROR: Could not modify Work Order', **result)
        else:
            module.exit_json(**result)

    elif module.params['action'] == 'get_categories':
        for i in range(CONST_NUMRETRIES):
            try:
                response = get_categories(module.params["apibase"], module.params["data"]["company"], module.params["data"]["technology"], module.params["data"]["defautl_technology"], module.params["headers"])
                if type(response) is dict and response['status_code'] == 200:
                    result['changed'] = True
                    result['assignments'] = response
                    #result['message'] = response.text
                    break
                elif (type(response) is not dict and response.status_code >= 400) or (type(response) is dict and response['status_code'] >= 400):
                    result['message'] = response.text
                    refreshtoken(module.params['token_dir'], module.params['apibase'], module.params['user'], module.params['password'], module.params["headers"])
            except Exception as e:
                result['module_traceback'] = traceback.format_exc()
                result['message'] = str(e)
        if not result['changed']:
            result['message'] += CONST_MESSAGE
            module.fail_json(msg='ERROR: Could not Get Categories', **result)
        else:
            module.exit_json(**result)

def main():
    run_module()

if __name__ == '__main__':
    main()
