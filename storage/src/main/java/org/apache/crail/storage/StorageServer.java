/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.crail.storage;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.crail.CrailStorageClass;
import org.apache.crail.conf.Configurable;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.conf.CrailConstants;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.metadata.DataNodeStatistics;
import org.apache.crail.metadata.DataNodeStatus;
import org.apache.crail.rpc.RpcClient;
import org.apache.crail.rpc.RpcConnection;
import org.apache.crail.rpc.RpcDispatcher;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;


public interface StorageServer extends Configurable, Runnable  {


	public abstract StorageResource allocateResource() throws Exception;
	public abstract boolean isAlive();
	public abstract void prepareToShutDown();
	public abstract InetSocketAddress getAddress();

	
	public static void main(String[] args) throws Exception {
		Logger LOG = CrailUtils.getLogger();
		CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
		CrailConstants.updateConstants(conf);
		CrailConstants.printConf();
		CrailConstants.verify();
		
		int splitIndex = 0;
		for (String param : args){
			if (param.equalsIgnoreCase("--")){
				break;
			} 
			splitIndex++;
		}
		
		//default values
		StringTokenizer tokenizer = new StringTokenizer(CrailConstants.STORAGE_TYPES, ",");
		if (!tokenizer.hasMoreTokens()){
			throw new Exception("No storage types defined!");
		}
		String storageName = tokenizer.nextToken();
		int storageType = 0;
		HashMap<String, Integer> storageTypes = new HashMap<String, Integer>();
		storageTypes.put(storageName, storageType);
		for (int type = 1; tokenizer.hasMoreElements(); type++){
			String name = tokenizer.nextToken();
			storageTypes.put(name, type);
		}
		int storageClass = -1;
		

		if (args != null) {
			Option typeOption = Option.builder("t").desc("storage type to start").hasArg().build();
			Option classOption = Option.builder("c").desc("storage class the server will attach to").hasArg().build();
			Options options = new Options();
			options.addOption(typeOption);
			options.addOption(classOption);
			CommandLineParser parser = new DefaultParser();
			
			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, splitIndex));
				if (line.hasOption(typeOption.getOpt())) {
					storageName = line.getOptionValue(typeOption.getOpt());
					storageType = storageTypes.get(storageName).intValue();
				}				
				if (line.hasOption(classOption.getOpt())) {
					storageClass = Integer.parseInt(line.getOptionValue(classOption.getOpt()));
				}					
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Storage tier", options);
				System.exit(-1);
			}
		}
		if (storageClass < 0){
			storageClass = storageType;
		}

		StorageTier storageTier = StorageTier.createInstance(storageName);
		if (storageTier == null){
			throw new Exception("Cannot instantiate datanode of type " + storageName);
		}

		StorageServer server = storageTier.launchServer();

		
		String extraParams[] = null;
		splitIndex++;
		if (args.length > splitIndex){
			extraParams = new String[args.length - splitIndex];
			for (int i = splitIndex; i < args.length; i++){
				extraParams[i-splitIndex] = args[i];
			}
		}

		server.init(conf, extraParams);
		server.printConf(LOG);



		Thread thread = new Thread(server);
		thread.start();

		RpcClient rpcClient = RpcClient.createInstance(CrailConstants.NAMENODE_RPC_TYPE);

		rpcClient.init(conf, args);
		rpcClient.printConf(LOG);					
		
		ConcurrentLinkedQueue<InetSocketAddress> namenodeList = CrailUtils.getNameNodeList();


		ConcurrentLinkedQueue<RpcConnection> connectionList = new ConcurrentLinkedQueue<RpcConnection>();
		while(!namenodeList.isEmpty()){

			InetSocketAddress address = namenodeList.poll();

			RpcConnection connection = rpcClient.connect(address);
			connectionList.add(connection);
		}

		RpcConnection rpcConnection = connectionList.peek();
		if (connectionList.size() > 1){
			rpcConnection = new RpcDispatcher(connectionList);
		}		
		LOG.info("connected to namenode(s) " + rpcConnection.toString());

		//storage rpc
		StorageRpcClient storageRpc = new StorageRpcClient(storageType, CrailStorageClass.get(storageClass), server.getAddress(), rpcConnection);
		DataNodeInfo dnInfo=storageRpc.getdnInfo();
		LOG.info("dnInfo " + dnInfo);

		LOG.info("rpcclient.hashcode " + rpcClient.hashCode());



		HashMap<Long, Long> blockCount = new HashMap<Long, Long>();
		long sumCount = 0;
		long lba = 0;
		while (server.isAlive()) {

			StorageResource resource = server.allocateResource();

			if (resource == null){
				break;
			}
			else {
				storageRpc.setBlock(lba, resource.getAddress(), resource.getLength(), resource.getKey());
				lba += (long) resource.getLength();

				DataNodeStatistics stats = storageRpc.getDataNode();
				long newCount = stats.getFreeBlockCount();
				long serviceId = stats.getServiceId();

				long oldCount = 0;
				if (blockCount.containsKey(serviceId)){
					oldCount = blockCount.get(serviceId);
				}
				long diffCount = newCount - oldCount;
				blockCount.put(serviceId, newCount);
				sumCount += diffCount;

				LOG.info("111 datanode statistics, freeBlocks " + sumCount);
			}
		}

		while (server.isAlive()) {
			int test=0;
			//NetUsage tp=new NetUsage();
			//int tpuse=  tp.get();
			//LOG.info("tpuse:"+tpuse);
			rpcConnection.heartbeat(dnInfo,test);
			test++;
			DataNodeStatistics stats = storageRpc.getDataNode();
			long newCount = stats.getFreeBlockCount();
			long serviceId = stats.getServiceId();
			short status = stats.getStatus().getStatus();

			long oldCount = 0;
			if (blockCount.containsKey(serviceId)){
				oldCount = blockCount.get(serviceId);
			}
			long diffCount = newCount - oldCount;
			blockCount.put(serviceId, newCount);
			sumCount += diffCount;

			LOG.info("datanode statistics, freeBlocks " + sumCount);
			processStatus(server, rpcConnection, thread, status);
			//Thread.sleep(CrailConstants.STORAGE_KEEPALIVE*1000);

		}
	}



	public static void processStatus(StorageServer server, RpcConnection rpc, Thread thread, short status) throws Exception {
		if (status == DataNodeStatus.STATUS_DATANODE_STOP) {
			server.prepareToShutDown();
			rpc.close();
			// interrupt sleeping thread
			try {
				thread.interrupt();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

}
 class NetUsage{

	Logger LOG = CrailUtils.getLogger();
	private static NetUsage INSTANCE = new NetUsage();
	private final static float TotalBandwidth = 5000;
	NetUsage(){}
	public static NetUsage getInstance(){
		return INSTANCE;
	}
	public int get() {
		// LOG.info("begin get tp");

		int netUsage = 0;
		Process pro1,pro2;
		Runtime r = Runtime.getRuntime();
		try {
			String command = "cat /proc/net/dev";

			long startTime = System.currentTimeMillis();
			pro1 = r.exec(command);
			BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
			String line = null;
			long inSize1 = 0, outSize1 = 0;
			while((line=in1.readLine()) != null){
				line = line.trim();
				if(line.startsWith("ens5")){
					//System.out.println(line);
					String[] temp = line.split("\\s+");
					//System.out.println("temp: "+temp.length+"temp[0]="+temp[0]);

					inSize1 = Long.parseLong(temp[1]); //Receive bytes
					outSize1 = Long.parseLong(temp[9]);             //Transmit bytes
					break;
				}
			}
			in1.close();
			pro1.destroy();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				StringWriter sw = new StringWriter();
				e.printStackTrace(new PrintWriter(sw));
				System.out.println("NetUsage  InterruptedException. " + e.getMessage());
				System.out.println(sw.toString());
			}
			long endTime = System.currentTimeMillis();
			pro2 = r.exec(command);
			BufferedReader in2 = new BufferedReader(new InputStreamReader(pro2.getInputStream()));
			long inSize2 = 0 ,outSize2 = 0;
			while((line=in2.readLine()) != null){
				line = line.trim();
				if(line.startsWith("ens5")){
					//System.out.println(line);
					String[] temp = line.split("\\s+");

					inSize2 = Long.parseLong(temp[1]);
					outSize2 = Long.parseLong(temp[9]);
					break;
				}
			}
			if(inSize1 != 0 && outSize1 !=0 && inSize2 != 0 && outSize2 !=0){
				float interval = (float)(endTime - startTime)/1000;

				float curRate = (float)(inSize2 - inSize1 + outSize2 - outSize1)*8/(1000000*interval);
				netUsage = (int)( 100-100*curRate/TotalBandwidth);
				//System.out.println("this node " + curRate + "Mbps");
			    //System.out.println("usage " + netUsage);
			}
			in2.close();
			pro2.destroy();
		} catch (IOException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			System.out.println("NetUsage  InstantiationException. " + e.getMessage());
			System.out.println(sw.toString());
		}
		return netUsage;
	}
}



