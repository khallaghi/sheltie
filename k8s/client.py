from __future__ import print_function
import os
import yaml
from kubernetes.client.rest import ApiException
from kubernetes import client
from kubernetes import config as k8s_config
from constants import KIND, COMMAND
import config
import logging
FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s in function: %(func_name) \n' +\
        ' k8s_api_instance: %(api_instance)\n ' +\
        'k8s_api_func: %(api_func)\n message: %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()


ROOT_DIR = config.DEFAULT['ROOT_DIR']


kubectl_config = config.DEFAULT['KUBECTL_CONFIG']
k8s_config.load_kube_config(os.path.join(kubectl_config))


def create_job(msg):
    yaml_file = open(ROOT_DIR + msg.file_name, 'r+')
    yaml_conf = yaml.load(yaml_file)
    api_instance = client.BatchV1Api(client.ApiClient())
    try:
        api_response = api_instance.create_namespaced_job(msg.namespace, yaml_conf)
        logger.info(api_response)
    except ApiException as e:
        extra = {
            'api_instance':'BatchV1Api',
            'api_func':'create_namespaced_job',
            'func_name':'create_job'
        }
        #logger.error("error_message: %s", e, extra=extra)
        raise e


def delete_job(msg):
    yaml_file = open(ROOT_DIR + msg['file_name'], 'r+')
    yaml_conf = yaml.load(yaml_file)
    api_instance = client.BatchV1Api(client.ApiClient())
    try:
        api_response = api_instance.delete_namespaced_job(msg.name, msg.namespace, yaml_conf)
        logger.info(api_response)
    except ApiException as e:
        extra = {
            'api_instance':'BatchV1Api',
            'api_func':'delete_namespaced_job',
            'func_name':'delete_job'
        }
        #logger.error("error_message: %s", e, extra=extra)
        raise e


def create_deployment(msg):
    yaml_file = open(ROOT_DIR + msg.file_name, 'r+')
    yaml_conf = yaml.load(yaml_file)
    k8s_beta = client.ExtensionsV1beta1Api()
    try:
        api_response = k8s_beta.create_namespaced_deployment(msg.namespace, yaml_conf)
        logger.info(api_response)
    except ApiException as e:
        extra = {
            'api_instance': 'ExtensionsV1beta1Api',
            'api_func': 'create_namespaced_deployment ',
            'func_name': 'create_deployment'
        }
        #logger.error("error_message: %s", e, extra=extra)
        raise e


def delete_deployment(msg):
    yaml_file = open(ROOT_DIR + msg.file_name, 'r+')
    yaml_conf = yaml.load(yaml_file)
    k8s_beta = client.ExtensionsV1beta1Api()
    try:
        api_response = k8s_beta.delete_namespaced_deployment(msg.name, msg.namespace, yaml_conf)
        logger.info(api_response)
    except ApiException as e:
        extra = {
            'api_instance': 'ExtensionsV1beta1Api',
            'api_func': 'delete_namespaced_deployment',
            'func_name': 'delete_deployment'
        }
        #logger.error("error_message: %s", e, extra=extra)
        raise e


def handle_request(msg):
    if msg.kind == KIND['JOB']:
        if msg.command == COMMAND['CREATE']:
            return create_job(msg)
        if msg.command == COMMAND['DELETE']:
            return delete_job(msg)
    if msg.kind == KIND['DEPLOYMENT']:
        if msg.command == COMMAND['CREATE']:
            return create_deployment(msg)
        if msg.command == COMMAND['DELETE']:
            return delete_deployment(msg)
