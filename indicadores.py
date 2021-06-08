#!/usr/bin/python

# Copyright: (c) 2018, Terry Jones <terry.jones@example.org>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
from ansible.module_utils.basic import AnsibleModule
from dateutil.parser import parse
from datetime import datetime
import psycopg2
import time
import random
import paramiko

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.

__metaclass__ = type

DOCUMENTATION = r'''
---
module: bd_indicadores
short_description: Insert data into indicadores_db database (TimeScale DB)

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

def validateData(data):
  global GLOBAL_MESSAGE
  global GLOBAL_ERRORS
  global CONST_DEFAULTS
  global CONST_REQUIRED
  # Check for required keys, defaults, empty values and unrecognized keys.
  for i in CONST_REQUIRED:
    if i not in data.keys():
      GLOBAL_MESSAGE += "ERROR: Missing required data key: '"+i+"'\n"
      GLOBAL_ERRORS += 1
    elif not data[i]:
      GLOBAL_MESSAGE += "ERROR: Missing required value, data key: '"+i+"'\n"
      GLOBAL_ERRORS += 1
  for i in data:
    if i not in CONST_DEFAULTS.keys() and not i in CONST_REQUIRED and not data[i]:
      GLOBAL_MESSAGE += "WARNING: Unrecognized data key: '"+i+"'\n"
  for i in CONST_DEFAULTS:
    if i not in data.keys() or not data[i]:
      data[i] = CONST_DEFAULTS[i]  
  # Requided Data validation Checks
  try:
    datetime.fromtimestamp(int(data["playbook_start_timestamp"])//1)
  except ValueError as e:
    #print(str(e))
    GLOBAL_MESSAGE += "ERROR: playbook_start_timestamp value must be a valid linux epoch timestamp format\n"
    GLOBAL_ERRORS += 1
  return data;

def getManTime(config,autid):
  global GLOBAL_MESSAGE
  global GLOBAL_ERRORS
  try:
    if "port" not in config.keys():
      config["port"] = 5432
    if config["method"] == "ssh":
      query = "select mantime from automatizacion where id="+str(autid)+";"
      completecommand = "PGPASSWORD=\""+config["db_password"]+"\" psql -h "+config["db_server"]+" -U "+config["db_user"]+" -d "+config["db_name"]+" -p "+str(config["db_port"])+" -tc \""+query+"\"" +" | head -1 | tr -d ' '"
      p = paramiko.SSHClient()
      p.set_missing_host_key_policy(paramiko.AutoAddPolicy())   # This script doesn't work for me unless this line is added!
      p.connect(config["ssh_server"], port=config["ssh_port"], username=config["ssh_user"], password=config["ssh_password"])
      stdin, stdout, stderr = p.exec_command(completecommand)
      opt = stdout.readlines()
      opt = "".join(opt)
      try:
        mantime=float(opt)
        return mantime
      except ValueError:
        GLOBAL_MESSAGE += "Record with aut_id = "+str(autid)+" does NOT exist in catalog OR returned an invalid 'mantime' value ("+opt+")"
        GLOBAL_ERRORS += 1 
    elif config["method"] == "postgres":
      conn = psycopg2.connect(dbname=config["db_name"], user=config["db_user"], password=config["db_password"], host=config["db_server"],port=config["db_port"])
      cursor = conn.cursor()
      query="select mantime from automatizacion where id=%s"
      values = str(autid)
      cursor.execute(query,values)
      result=cursor.fetchone()
      try:
        mantime=float(result[0])
        return mantime
      except ValueError:
        GLOBAL_MESSAGE += "Record with aut_id = "+str(autid)+" does NOT exist in catalog OR returned an invalid 'mantime' value ("+opt+")"
        GLOBAL_ERRORS += 1
      return mantime
  except Exception as e:
    GLOBAL_MESSAGE += str(e)
    GLOBAL_ERRORS += 1
 
def insertData(config,data):
  global GLOBAL_MESSAGE
  global GLOBAL_ERRORS
  # Calculate variables to be inserted
  try:
    now = datetime.now()
    time = now.strftime("%Y-%m-%d %H:%M:%S")
    endtime = now.strftime('%s')
    autotime =  (int(endtime)//1 - int(data["playbook_start_timestamp"])//1)/3600
    mantime = getManTime(config,data["aut_id"])
    svtime = mantime - autotime
    svfte = svtime/150
    if autotime < 0 or svtime < 0:
      raise Exception("Exception: autotime or svtime or both are negative numbers.")
    transactionid = data["transaction_identifier"]+"."+now.strftime("%Y%m%d.%H%M%S.%f")
    if "port" not in config.keys():
      config["port"] = 5432
    if config["method"] == "ssh":
      query = "insert into indicadores (time, autid, autotime, svtime, transactionid, ticketid, svfte) values "
      values = "('"+time+"',"+str(data["aut_id"])+","+str(autotime)+","+str(svtime)+",'"+transactionid+"','"+data["woid"]+"',"+str(svfte)+");"
      completequery = query+values
      completecommand = "PGPASSWORD=\""+config["db_password"]+"\" psql -h "+config["db_server"]+" -U "+config["db_user"]+" -d "+config["db_name"]+" -p "+str(config["db_port"])+" -c \""+completequery+"\""
      p = paramiko.SSHClient()
      p.set_missing_host_key_policy(paramiko.AutoAddPolicy())   # This script doesn't work for me unless this line is added!
      p.connect(config["ssh_server"], port=config["ssh_port"], username=config["ssh_user"], password=config["ssh_password"])
      stdin, stdout, stderr = p.exec_command(completecommand)
      opt = stdout.readlines()
      opt = "".join(opt)
      err = stderr.readlines()
      err = "".join(err)
      if "INSERT 0 1" not in opt:
        GLOBAL_MESSAGE += "POSTGRESQL ERROR: stdout: "+opt+"; stderr: "+err
        GLOBAL_ERRORS += 1
    elif config["method"] == "postgres":
      conn = psycopg2.connect(dbname=config["db_name"], user=config["db_user"], password=config["db_password"], host=config["db_server"],port=config["db_port"])
      cursor = conn.cursor()
      query = "insert into indicadores (time, autid, autotime, svtime, transactionid, ticketid, svfte) values (%s, %s, %s, %s, %s, %s, %s)"
      values = [time,data["aut_id"],autotime,svtime,transactionid,data["woid"],svfte]
      cursor.execute(query,values)
      conn.commit()
  except Exception as e:
    GLOBAL_MESSAGE += str(e)
    GLOBAL_ERRORS += 1
  
    

# Module constants definition
GLOBAL_MESSAGE = ""
GLOBAL_ERRORS = 0
CONST_REQUIRED = ["aut_id", "playbook_start_timestamp"]
CONST_DEFAULTS = {
    "transaction_identifier" : "XXX",
    "client" : "MULTICLIENTE",
    "platform" : "AWX",
    "creator" : "AUTOMATIZACION",
    "type" : "SCRIPTING",
    "function" : "CHECK LIST",
    "specialist" : "INGENIERO",
    "exec_type" : "SOBRE DEMANDA",
    "manual_execs" : "1",
    "auto_execs" : "1",
    "woid" : "0",
    "ci": "OTROS",
    "technology": "OTROS"
  }

def run_module():
    # define available arguments/parameters a user can pass to the module
    module_args = dict(
        config=dict(type='dict',required=True, no_log=True),
        data=dict(type='dict', required=True)
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

    # manipulate or modify the state as
    # needed (this is going to be the
    # part where your module will do what it needs to do)
    result['original_message'] = "Requested create record in database'"
    
    # made any modifications to your target
    try:
      validatedData = validateData(module.params["data"])
      #print("Data validated, inserting data...")
      insertData(module.params["config"],validatedData)
      if GLOBAL_ERRORS > 0:
        result["message"] = "ERROR: Could not insert data into database."
        module.fail_json(msg=GLOBAL_MESSAGE, **result)
      else:
        result["message"] = "SUCCESSFULLY inserted data into database"
        result["changed"] = True
        module.exit_json(**result)
    except Exception as e:
      result["message"] = "ERROR: Could not insert data into database: "+str(e)
      module.fail_json(msg=GLOBAL_MESSAGE, **result)

def main():
    run_module()


if __name__ == '__main__':
    main()
