package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.exceptions.UnirestException;



import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Kunal on 12/20/2014.
 */
public class CRDTClient {

    private ArrayList<DistributedCacheService> cacheServers;
	
	
    private CountDownLatch decrementLatch;

    public CRDTClient() {
        this.cacheServers = new ArrayList<DistributedCacheService>();
        cacheServers.add(new DistributedCacheService("http://localhost:3000"));
        cacheServers.add(new DistributedCacheService("http://localhost:3001"));
        cacheServers.add(new DistributedCacheService("http://localhost:3002"));
    }

	    public String get(long key) throws InterruptedException, UnirestException, IOException {
        this.decrementLatch = new CountDownLatch(cacheServers.size());
		
		
		
        final Map<DistributedCacheService, String> resultMap = new HashMap<DistributedCacheService, String>();
		
        for (final DistributedCacheService cacheServer : cacheServers) {
            Future<HttpResponse<JsonNode>> future = Unirest.get(cacheServer.getCacheServerUrl() + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        public void failed(UnirestException e) {
                            System.out.println("Request Failed to Execute");
                            decrementLatch.countDown();
                        }

                        public void completed(HttpResponse<JsonNode> response) {
                            resultMap.put(cacheServer, response.getBody().getObject().getString("value"));
                            System.out.println("Request Successful "+cacheServer.getCacheServerUrl());
                            decrementLatch.countDown();
                        }

                        public void cancelled() {
                            System.out.println("Request Cancelled");
                            decrementLatch.countDown();
                        }
                    });
        }
        this.decrementLatch.await(3, TimeUnit.SECONDS);
        final Map<String, Integer> countMap = new HashMap<String, Integer>();
		
		
		
		
        int maximumCount = 0;
        for (String value : resultMap.values()) {
            int count = 1;
            if (countMap.containsKey(value)) {
                count = countMap.get(value);
                count++;
            }
            if (maximumCount < count)
                maximumCount = count;
            countMap.put(value, count);
        }

        String value = this.getCRDTValue(countMap, maximumCount);

        if (maximumCount != this.cacheServers.size()) {
            for (Map.Entry<DistributedCacheService, String> cachedData : resultMap.entrySet()) {
                if (!value.equals(cachedData.getValue())) {
                    System.out.println("Read Repairing "+cachedData.getKey());
                    HttpResponse<JsonNode> response = Unirest.put(cachedData.getKey() + "/cache/{key}/{value}")
                            .header("accept", "application/json")
                            .routeParam("key", Long.toString(key))
                            .routeParam("value", value)
                            .asJson();
                }
            }
            for (DistributedCacheService cacheServer : this.cacheServers) {
                if (resultMap.containsKey(cacheServer)) continue;
                System.out.println("Read Repairing "+cacheServer.getCacheServerUrl());
                HttpResponse<JsonNode> response = Unirest.put(cacheServer.getCacheServerUrl() + "/cache/{key}/{value}")
                        .header("accept", "application/json")
                        .routeParam("key", Long.toString(key))
                        .routeParam("value", value)
                        .asJson();
            }
        } else {
            System.out.println("Read Repair Not Performed");
        }
        Unirest.shutdown();
        return value;
    }


    public String getCRDTValue(Map<String, Integer> map, int value) {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (value == entry.getValue()) return entry.getKey();
        }
        return null;
    }
	
	
    public boolean put(long key, String value) throws InterruptedException, IOException {
        final AtomicInteger successCount = new AtomicInteger(0);
        this.decrementLatch = new CountDownLatch(cacheServers.size());
        final ArrayList<DistributedCacheService> writtenServerList = new ArrayList<DistributedCacheService>(3);
        for (final DistributedCacheService cacheServer : cacheServers) {
            Future<HttpResponse<JsonNode>> future = Unirest.put(cacheServer.getCacheServerUrl() + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value)
                    .asJsonAsync(new Callback<JsonNode>() {

                        public void failed(UnirestException e) {
                            System.out.println("Request Failed to Execute "+cacheServer.getCacheServerUrl());
                            decrementLatch.countDown();
                        }

                        public void completed(HttpResponse<JsonNode> response) {
                            int count = successCount.incrementAndGet();
                            writtenServerList.add(cacheServer);
                            System.out.println("Request Successful"+cacheServer.getCacheServerUrl());
                            decrementLatch.countDown();
                        }

                        public void cancelled() {
                            System.out.println("Request Cancelled");
                            decrementLatch.countDown();
                        }

                    });
        }
        this.decrementLatch.await();
        if (successCount.intValue() > 1) {
            return true;
        } else {
            this.decrementLatch = new CountDownLatch(writtenServerList.size());
            for (final DistributedCacheService cacheServer : writtenServerList) {
                cacheServer.remove(key);
                System.out.println("Deleted key: "+key+" from : "+cacheServer.getCacheServerUrl());
            }
            this.decrementLatch.await(3, TimeUnit.SECONDS);
            Unirest.shutdown();
            return false;
        }
    }

}