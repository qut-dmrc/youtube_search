from yaml import load
from os import getcwd

cfg = None

try:
    with open("config.yml", 'r') as ymlfile:
        cfg = load(ymlfile)
except Exception as e:
    cwd = getcwd()
    print(
        "Unable to load config file. Try to load it manually instead using load_config. \nWorking dir: {}. \nError: {}".format(
            cwd, e))


def load_config(filename):
    global cfg

    with open(filename, 'r') as ymlfile:
        cfg = load(ymlfile)