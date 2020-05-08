from yaml import load
from os import getcwd, environ

cfg = None

try:
    with open("config.yml", 'r') as ymlfile:
        cfg = load(ymlfile)

    # set environment variables
    if 'observatory' in cfg:
        environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_key_file=cfg['BQ_KEY_FILE']
        environ['GOOGLE_CLOUD_PROJECT'] = cfg['PROJECT_ID']

except Exception as e:
    cwd = getcwd()
    print(
        "Unable to load config file. Try to load it manually instead using load_config. \nWorking dir: {}. \nError: {}".format(
            cwd, e))


def load_config(filename):
    global cfg

    with open(filename, 'r') as ymlfile:
        cfg = load(ymlfile)