#!/usr/bin/python

#
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
#

# TODO:
# 1. ./LingccfsDeploy sync conf jar
# 2. ./LingccfsDeploy

'''
Created on 2013-2-4

@author: lingkun
'''

import getopt
import glob
import sys, os
import shutil
import subprocess, shlex
import logging

REMOTE_ACCOUNT="root"
REMOTE_HADOOP_HOME = "/root/hbase_test/hadoop-1.0.4/"
REMOTE_HBASE_HOME = "/root/hbase_test/hbase-0.94.3/"

LINGCCFS_CONF_HOME = "/home/lingkun/projects/hadoop/hadoop-lingccfs/"
LINGCCFS_CONF_HOME += "lingccfs_conf/hadoop-1.0.4/"
HADOOP_SRC_DIR = os.environ.get('HADOOP_SRC_HOME','')

HBASE_CONF_DIR = "/home/lingkun/projects/hadoop/hadoop-lingccfs/"
HBASE_CONF_DIR += "lingccfs_conf/hbase-0.94.3/conf_lingccfs/"

logger = logging.getLogger('LingccfsDeploy')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def usage():
  print "usage: python LingccfsDeploy.py"

def addSlash(s):
  if not (s[-1] == '/'):
    s = s + '/'
  return s

def whereis(program):
  abspath = None
  for path in (os.environ.get('PATH', '')).split(':'):
    abspath = os.path.join(path, program)
    if os.path.exists(abspath) and not os.path.isdir(abspath):
      return abspath
  return None

def getLatestJar(targetdir):
  latestJar = None
  lingccfsJar = glob.glob(targetdir + "*.jar")

  if len(lingccfsJar) == 0:
    logger.error("No LingccFS jar file found in %s ... exiting" % (targetdir) )
    return None

  # pick up the latest jar file - just in case ...
  stat = latestJar = None
  ctime = 0
  for jar in lingccfsJar:
    stat = os.stat(jar)
    if stat.st_ctime > ctime:
      latestJar = jar
      ctime = stat.st_ctime
  return latestJar

# Get Source
def package_get_source( package_name ):
  location = whereis('wget')

  if location == None:
    logger.error( "Cannot find wget to download hadoop, hbase packages" )
    logger.error( "please install it or fix your PATH environ")
    return None


def package_untar( package_name ):
  return None


def package_deploy( package_name ):
  return None


# build the glusterfs hadoop plugin using maven
def build_jar():
  location = whereis('mvn')

  if location == None:
    logger.error( "Cannot find maven to build lingccfs hadoop jar" )
    logger.error ( "please install maven or if it's already installed then fix your PATH environ" )
    return None

  # do a clean packaging
  targetdir = "./target/"
  if os.path.exists(targetdir) and os.path.isdir(targetdir):
    logger.info( "Build JAR:Cleaning up directories ... [ " + targetdir + " ]" )
    shutil.rmtree(targetdir)

  logger.info ("Build JAR:Building lingccfs jar ...")
  process = subprocess.Popen(['package'], shell=True,
      executable=location, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

  try:
    (pout, perr) = process.communicate()
  except:
    process.wait()
    if not process.returncode == 0:
      logger.error ("Building lingccfs jar failed")
      return None

  latestJar = getLatestJar(targetdir)
  return latestJar

def rcopy(f, accnt,  host, libdir):
  logger.info ( "* doing remote copy to host %s" % (host) )
  scpCmd = "scp %s %s@%s:%s" % (f, accnt, host, libdir)
  os.system(scpCmd);


def deployToRemote(f, accnt, local_confdir, remote_confdir, remote_libdir,
    cc=True, cm=True, he=True, le=True):
  slavefile = local_confdir + "slaves"
  ccFile = local_confdir + "core-site.xml"
  cmFile = local_confdir + "mapred-site.xml"
  heFile = local_confdir + "hadoop-env.sh"
  leFile = local_confdir + "log4j.properties"

  sf = open(slavefile, 'r')
  for host in sf.readlines():
    host = host.rstrip('\n')
    logger.info( "Deploying %s on %s ..." % (os.path.basename(f), host) )
    rcopy(f, accnt, host, remote_libdir)

    if cc:
      logger.info( "Deploying [%s] on %s ..." % (os.path.basename(ccFile), host))
      rcopy(ccFile, accnt, host, remote_confdir)

    if cm:
      logger.info( "Deploying [%s] on %s ..." % (os.path.basename(cmFile), host))
      rcopy(cmFile, accnt, host, remote_confdir)

    if he:
      logger.info( "Deploying [%s] on %s ..." % (os.path.basename(heFile), host) )
      rcopy(heFile, accnt, host, remote_confdir)

    if le:
      logger.info( "Deploying [%s] on %s ..." % (os.path.basename(leFile), host) )
      rcopy(leFile, accnt, host, remote_confdir)

    logger.info( "%s Deploy Done" %(host) )
    sf.close()


## Main
if __name__ == '__main__':
  needbuild = True
  sync_cc=False
  sync_cm=False
  sync_he=False
  sync_le=True

  if needbuild:
    jar = build_jar()
    if jar == None:
      logger.error( "lingccfs jar build error." )
      sys.exit(1)
  else:
    jar = getLatestJar('./target/')
    if jar == None:
      logger.error( "Maybe you want to build it ? -b option" )
      sys.exit(1)
  logger.info( "*** Deploying %s *** " % (jar) )

  LINGCCFS_CONF_NAME = "conf_alllingccfs/"
  HDFS_CONF_NAME = "conf_hdfs_tmplocal/"

  CONF_NAME=LINGCCFS_CONF_NAME

  remote_accnt= REMOTE_ACCOUNT
  remote_hadoop_conf = REMOTE_HADOOP_HOME + "conf/"
  remote_hadoop_lib = REMOTE_HADOOP_HOME + "lib/"
  local_hadoop_conf = LINGCCFS_CONF_HOME + CONF_NAME
  local_hadoop_build = HADOOP_SRC_DIR + "build/"
  local_hadoop_home = os.environ['HADOOP_HOME']

  if not ( len(local_hadoop_home) > 0
      and os.path.exists(local_hadoop_home)
      and os.path.isdir(local_hadoop_home) ):
    logger.error ( "$HADOOP_HOME="+local_hadoop_home +", please check it." )
    sys.exit(1)

  local_hadoop_lib = local_hadoop_home+ "/lib/"

  logger.info ( "Scanning hadoop master and slave file for host(s) to deploy" )
  deployToRemote(jar, remote_accnt, local_hadoop_conf, remote_hadoop_conf,
      remote_hadoop_lib, sync_cc, sync_cm, sync_he,sync_le)

