import configparser
import os

# repair case problem in configparser package
class MyConfigParser(configparser.ConfigParser):
    def __init__(self, defaults=None):
        configparser.ConfigParser.__init__(self, defaults=defaults)
    def optionxform(self, optionstr):
        return optionstr


def modifyConfig(file,field,key,value):
    parser = MyConfigParser()
    parser.read(file)
    parser.set(field,key,value)
    parser.write(open(file,'w'))


def load(db,dbfilename,workload,resultfile="-1"):
    os.system("sudo rm -rf {0}".format(dbfilename))
    if resultfile=="-1":
        os.system("./ycsbc -db {0} -dbfilename {1} -threads 1 -P {2} -skipLoad false".format(db,dbfilename,workload))
    else:
        os.system("./ycsbc -db {0} -dbfilename {1} -threads 1 -P {2} -skipLoad false > {3}".format(db,dbfilename,workload,resultfile))

def run(db,dbfilename,workload,resultfile="-1"):
    if resultfile=="-1":
        os.system("./ycsbc -db {0} -dbfilename {1} -threads 1 -P {2} -skipLoad true".format(db,dbfilename,workload))
    else:
        os.system("./ycsbc -db {0} -dbfilename {1} -threads 1 -P {2} -skipLoad true > {3}".format(db,dbfilename,workload,resultfile))
