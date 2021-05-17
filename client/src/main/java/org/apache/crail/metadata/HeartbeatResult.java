package org.apache.crail.metadata;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class HeartbeatResult {

    public static final int CSIZE = 8;
    int cpuUsage;
    int  netUsage;

    public  HeartbeatResult(int cpuUsage, int netUsage){
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
