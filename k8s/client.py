from __future__ import print_function
import os
import yaml
from kubernetes.client.rest import ApiException
from kubernetes import client
from kubernetes import config as k8s_config
from pprint import pprint
import configparser


KIND = {
    'JOB': 'job',
    'DEPLOYMENT': 'deployment'
}

COMMAND = {
    'DELETE': 'delete',
    'CREATE': 'create'
}

'''
    msg is a json message that received from rabbitmq 
    with this structure.
    msg = {
        "file_name": "video-ftp.yaml",
        "kind": "job",
        "namespace": "default"
    } 
    
'''
config = configparser.ConfigParser()
config.read('config.ini')
ROOT_DIR = config['DEFAULT']['ROOT_DIR']


kubectl_config = config['DEFAULT']['KUBECTL_CONFIG']
k8s_config.load_kube_config(os.path.join(kubectl_config))


def create_job(msg):
    yaml_file = open(ROOT_DIR + msg['file_name'], 'r+')
    yaml_conf = yaml.load(yaml_file)
    api_instance = client.BatchV1Api(client.ApiClient())
    try:
        api_response = api_instance.create_namespaced_job(msg['namespace'], yaml_conf)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)


def delete_job(msg):
    yaml_file = open(ROOT_DIR + msg['file_name'], 'r+')
    yaml_conf = yaml.load(yaml_file)
    api_instance = client.BatchV1Api(client.ApiClient())
    try:
        api_response = api_instance.delete_namespaced_job(msg['name'], msg['namespace'], yaml_conf)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)


def create_deployment(msg):
    yaml_file = open(ROOT_DIR + msg['file_name'], 'r+')
    yaml_conf = yaml.load(yaml_file)
    k8s_beta = client.ExtensionsV1beta1Api()
    try:
        api_response = k8s_beta.create_namespaced_deployment(msg['namespace'], yaml_conf)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)


def delete_deployment(msg):
    yaml_file = open(ROOT_DIR + msg['file_name'], 'r+')
    yaml_conf = yaml.load(yaml_file)
    k8s_beta = client.ExtensionsV1beta1Api()
    try:
        api_response = k8s_beta.delete_namespaced_deployment(msg['name'], msg['namespace'], yaml_conf)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)


def handle_request(msg):
    print(msg)
    if msg['kind'] == KIND['JOB']:
        if msg['command'] == COMMAND['CREATE']:
            create_job(msg)
        if msg['command'] == COMMAND['DELETE']:
            delete_job(msg)
    if msg['kind'] == KIND['DEPLOYMENT']:
        if msg['command'] == COMMAND['CREATE']:
            create_deployment(msg)
        if msg['command'] == COMMAND['DELETE']:
            delete_deployment(msg)
