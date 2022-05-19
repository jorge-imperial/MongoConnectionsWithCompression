package com.example.zlibcompression;

import com.mongodb.Block;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;


import java.util.concurrent.ThreadLocalRandom;


public class ReaderThread implements Runnable {

    private  static Logger logger = LogManager.getLogger(ReaderThread.class);
    private int count;

    public ReaderThread( int count) {
         this.count = count;
    }


    public void run() {

        MongoCollection<Document> collection = ZlibCompressionApplication.getMongoClient().getDatabase("test").getCollection("products");


        for (int i=0;i<count; ++i ) {

            int skuId = ThreadLocalRandom.current().nextInt(1, (int) count);

            collection.find(new Document("skuId", skuId)).forEach(new Block<Document>() {
                @Override
                public void apply(Document doc) {
                    logger.info("Thread: " + Thread.currentThread().getId());
                }
            });

        }

    }
}
