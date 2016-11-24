#!/usr/bin/env python
import os
import sys
import argparse
import subprocess

parser = argparse.ArgumentParser(description='initial mongodb dump to elasticsearch')
parser.add_argument('--config', required=True)
args = parser.parse_args(sys.argv[1:])

if not os.path.exists('lib/'):
    print 'lib folder not found, please run this command in application top directories'
else:
    config_files = map(lambda f: str(f), filter(lambda x: str(x).endswith('.conf'), os.listdir('.')))
    jar_files = map(lambda x: "lib/%s" % x, os.listdir('lib/'))
    commands = [
        'spark-submit',
        '--class us.pinguo.bigdata.sync.Bootstrap',
        '--master yarn',
        '--deploy-mode client',
        '--driver-memory 2g',
        '--executor-memory 2g',
        '--executor-cores 4',
        '--num-executors 12',
        '--files %s' % (','.join(config_files)),
        '--jars %s' % (','.join(jar_files)),
        'lib/mongodb-sync-initial.mongodb-sync-initial-0.1-SNAPSHOT.jar',
        '%s' % args.config
    ]
    subprocess.call(' '.join(commands), shell=True)
