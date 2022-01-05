package com.yh.thrift.impala.utils;

import org.apache.hadoop.hive.thrift.TFilterTransport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class YHTUGIAssumingTransport extends TFilterTransport {

    protected UserGroupInformation ugi;

    public YHTUGIAssumingTransport(TTransport wrapped, UserGroupInformation ugi) {
        super(wrapped);
        this.ugi = ugi;
    }

    public void open() throws TTransportException {
        try {
            this.ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() {
                    try {
                        YHTUGIAssumingTransport.this.wrapped.open();
                        return null;
                    } catch (TTransportException var2) {
                        throw new RuntimeException(var2);
                    }
                }
            });
        } catch (IOException var2) {
            throw new RuntimeException("Received an ioe we never threw!", var2);
        } catch (InterruptedException var3) {
            throw new RuntimeException("Received an ie we never threw!", var3);
        } catch (RuntimeException var4) {
            if (var4.getCause() instanceof TTransportException) {
                throw (TTransportException)var4.getCause();
            } else {
                throw var4;
            }
        }
    }

    /**
     * 空实现
     * @param var1
     * @throws TTransportException
     */
    public  void checkReadBytesAvailable(long var1) throws TTransportException{

    }

}
