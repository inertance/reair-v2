package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.incremental.DirectoryCopier;

import org.apache.hadoop.conf.Configuration;

public interface ClusterFactory {

  void setConf(Configuration conf);

  Cluster getSrcCluster() throws ConfigurationException;

  Cluster getDestCluster() throws ConfigurationException;

  DirectoryCopier getDirectoryCopier() throws ConfigurationException;
}
