LingccFS Hadoop Plugin
=======================

INTRODUCTION
------------

This document describes how to use LingccFS as a backing store with Hadoop.

Author: Ling Kun <lkun.lingcc@qq.com>

REQUIREMENTS
------------

* Supported OS is GNU/Linux
* LingccFS and Hadoop installed on all machines in the cluster
* Java Runtime Environment (JRE)
* Maven (needed if you are building the plugin from source)
* JDK (needed if you are building the plugin from source)

INSTALLATION
------------

* Building the plugin from source [Maven (http://maven.apache.org/) and JDK is required to build the plugin]

  Change to hadoop-lingccfs directory and build the plugin.

  # cd hadoop-lingccfs
  # mvn package

  On a successful build the plugin will be present in the `target` directory.

  # ls target/
  classes  lingccfs-1.0.4-0.5.jar  maven-archiver  surefire-reports  test-classes
               ^^^^^^^^^^^^^^^^^^

  Copy the plugin to lib/ directory in your $HADOOP_HOME dir.

  # cp target/lingccfs-1.0.4-0.5.jar $HADOOP_HOME/lib

  Copy the sample configuration file that ships with this source (conf/core-site.xml) to conf
  directory in your $HADOOP_HOME dir.

  # cp conf/core-site.xml $HADOOP_HOME/conf

CLUSTER INSTALLATION
--------------------

  In case it is tedious to do the above steps(s) on all hosts in the cluster; use the build-and-deploy.py script to
  build the plugin in one place and deploy it (along with the configuration file on all other hosts).

  This should be run on the host which is that hadoop master [Job Tracker].

* STEPS (You would have done Step 1 and 2 anyway while deploying Hadoop)

  1. Edit conf/slaves file in your hadoop distribution; one line for each slave.
  2. Setup password-less ssh b/w hadoop master and slave(s).
  3. Edit conf/core-site.xml with all lingccfs related configurations (see CONFIGURATION)
  4. Run the following
     # python ./build-and-deploy.py -b -d /path/to/hadoop/home -c

     This will build the plugin and copy it (and the config file) to all slaves (mentioned in $HADOOP_HOME/conf/slaves).

   Script options:
     -b : build the plugin
     -d : location of hadoop directory
     -c : deploy core-site.xml
     -m : deploy mapred-site.xml
     -h : deploy hadoop-env.sh


CONFIGURATION
-------------

  All plugin configuration is done in a single XML file (core-site.xml) with <name><value> tags in each <property>
  block.

  Brief explanation of the tunables and the values they accept (change them where-ever needed) are mentioned below

  name:  fs.lingccfs.impl
  value: org.apache.hadoop.fs.lingccfs.LingccFileSystem

         The default FileSystem API to use (there is little reason to modify this).

  name:  fs.default.name
  value: lingccfs:///NASMOUNTPATH

         The default name that hadoop uses to represent file as a URI (typically a server:port tuple). Use any host
         in the cluster as the server and any port number. This option has to be in server:port format for hadoop
         to create file URI; but is not used by plugin.

USAGE
-----

  Once configured, start Hadoop Map/Reduce daemons

  # cd $HADOOP_HOME
  # ./bin/start-mapred.sh

  If the map/reduce job/task trackers are up, all I/O will be done to LingccFS.

