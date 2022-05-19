package com.example.zlibcompression;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.concurrent.ThreadLocalRandom;

public class UpdateThread   implements Runnable {

    private  static Logger logger = LogManager.getLogger(UpdateThread.class);

    long count = 0;

    public UpdateThread( int updateCount) {
        this.count = updateCount;
    }


    public void run() {

        MongoCollection<Document> collection =  ZlibCompressionApplication.getMongoClient().getDatabase("test").getCollection("products");

        for (int i = 0; i< count; ++i ) {

            int skuId = ThreadLocalRandom.current().nextInt(1, (int) count);

            collection.updateMany(new Document("skuId", skuId),
                    new Document( "$set", new Document("thread", this.toString()  ))) ;


            logger.info("Thread: " + Thread.currentThread().getId());
        }

    }
}
