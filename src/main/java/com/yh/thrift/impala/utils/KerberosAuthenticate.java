package com.yh.thrift.impala.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Map;

public class KerberosAuthenticate {

    private Configuration configuration;
    private UserGroupInformation ugi;


    private KerberosAuthenticate() {
    }

    private void setConfig() {
        UserGroupInformation.setConfiguration(this.configuration);
    }

    public static KerberosAuthenticate initKerberos(
            String user, String keytabFile, Map<String, String> config) throws IOException {
        KerberosAuthenticate kerberosAuthenticate = new KerberosAuthenticate();
        Configuration configuration = kerberosAuthenticate.getConfiguration();
        if (null != config && !config.isEmpty()) {
            for (Map.Entry<String, String> entry : config.entrySet()) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        kerberosAuthenticate.setConfig();
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytabFile);
        kerberosAuthenticate.setUgi(ugi);

        return kerberosAuthenticate;
    }

    public Configuration getConfiguration() {
        this.configuration = configuration == null ? new Configuration() : configuration;
        return configuration;
    }

    public KerberosAuthenticate setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public KerberosAuthenticate setUgi(UserGroupInformation ugi) {
        this.ugi = ugi;
        return this;
    }

}
