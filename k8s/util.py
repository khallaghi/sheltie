
def change_yaml(yaml_config, args):
    if args is None:
        return yaml_config

    num_of_containers = len(yaml_config['spec']['template']['spec']['containers'])
    for i in range(num_of_containers):
        if isinstance(yaml_config['spec']['template']['spec']['containers'][i]['command'], list):
            yaml_config['spec']['template']['spec']['containers'][i]['command'].extend(args[i]['command'])
        else:
            yaml_config['spec']['template']['spec']['containers'][i]['command'] = args[i]['command']

        if isinstance(yaml_config['spec']['template']['spec']['containers'][i]['args'], list):
            yaml_config['spec']['template']['spec']['containers'][i]['args'].extend(args[i]['args'])
        else:
            yaml_config['spec']['template']['spec']['containers'][i]['args'] = args[i]['args']
    return yaml_config
