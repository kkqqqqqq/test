package org.apache.crail.metadata;

import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class HeartbeatResult {
    private static final Logger LOG = CrailUtils.getLogger();
    public static final int CSIZE = 8;

    public int  cpuUsage=0;
    public int  netUsage=0;
    public HeartbeatResult(){
        this.cpuUsage = 0;
        this.netUsage = 0;
    }
    public  HeartbeatResult(int cpuUsage, int netUsage){
        this();
        this.cpuUsage=cpuUsage;
        this.netUsage=netUsage;
    };

    public int getCpuUsage(){
        return this.cpuUsage;
    }

    public int getNetUsage(){
        return this.netUsage;
    }


    public int write(ByteBuffer buffer){
        buffer.putInt(cpuUsage);
        buffer.putInt(netUsage);
        return CSIZE;
    }

    public void update(ByteBuffer buffer) throws UnknownHostException {
        this.cpuUsage = buffer.getInt();
        this.netUsage = buffer.getInt();
    }
}
