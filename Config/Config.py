import configparser
import inspect
import os
import sys

CONFIG_FILE = 'Config.txt'


def str_to_class(str):
    return getattr(sys.modules[__name__], str)


def get_config_fp():
    name = str_to_class(inspect.stack()[0][3])
    fp = os.path.abspath(inspect.getfile(name))
    config_fp = os.path.join(os.path.dirname(fp), CONFIG_FILE)
    return config_fp


def get_config():
    config_fp = get_config_fp()
    parser = configparser.ConfigParser()
    parser.read(config_fp)
    return parser['DEFAULT']


def set_config(configs):
    assert(type(configs) == dict)
    config_fp = get_config_fp()
    parser = configparser.ConfigParser()
    parser.read(config_fp)
    for key in configs.keys():
        if key in parser['DEFAULT']:
            parser.set('DEFAULT', key, str(configs[key]))

    parser.write(open(config_fp, 'w'))
