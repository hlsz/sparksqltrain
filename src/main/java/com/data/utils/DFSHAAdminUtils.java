package com.data.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DFSHAAdminUtils extends DFSAdmin
{
    private Logger log = LoggerFactory.getLogger(DFSHAAdminUtils.class);
    private int rpcTimeoutForChecks = -1;
    private String nnidStr;
    private static String acitveNameNodeHostAndPort = null;
    public DFSHAAdminUtils(){};

    public DFSHAAdminUtils(Configuration conf, String nnidStr){
        super.setConf(conf);
        if (conf != null) {
            rpcTimeoutForChecks = conf.getInt(
                    CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
                    CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);
        }
        this.nnidStr = nnidStr;
    }


}
