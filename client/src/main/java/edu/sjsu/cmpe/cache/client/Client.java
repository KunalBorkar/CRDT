package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Distributed Cache Service Client");
        CRDTClient c = new CRDTClient();
        System.out.println("PUT {key: 1,value: a}");
        boolean successfulExecution = c.put(1, "a");
        if (successfulExecution) {
            System.out.println("PUT {key: 1,value: a} successful");
        } else {
            System.out.println("PUT {key: 1,value: a} unsuccessful");
        }
		
        Thread.sleep(30000);

        System.out.println("PUT {key: 1,value: b} ");
        successfulExecution = c.put(1, "b");
        if (successfulExecution) {
            System.out.println("PUT {key: 1,value: b} successful");
        } else {
            System.out.println("PUT {key: 1,value: b} unsuccessful");
        }
        Thread.sleep(30000);

        System.out.println("GET {key: 1,value: a} ");
        String cachedValue = c.get(1);
        System.out.println("GET {key: 1 value: "+ cachedValue + "}");
        System.out.println("Existing Distributed Cache Service Client");
    }
}
