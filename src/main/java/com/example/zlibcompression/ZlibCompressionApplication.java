package com.example.zlibcompression;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.MongoCompressor.createSnappyCompressor;
import static com.mongodb.MongoCompressor.createZlibCompressor;

@SpringBootApplication
public class ZlibCompressionApplication implements CommandLineRunner {

    private  static Logger logger = LogManager.getLogger(ZlibCompressionApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ZlibCompressionApplication.class, args);
    }

    private static MongoClient mClient = null;

    private static String hostName = "whale.local";

    public static MongoClient getMongoClient() {
        if (mClient == null) {
            List<MongoCompressor> compressors = new ArrayList<>();
            compressors.add(createZlibCompressor());
            mClient  = MongoClients.create(
                            MongoClientSettings.builder()
                                    .applyToClusterSettings(builder ->
                                        builder.hosts(Arrays.asList(
                                                new ServerAddress(hostName, 27017),
                                                new ServerAddress(hostName, 27018),
                                                new ServerAddress(hostName, 27019))))
                                    .compressorList( compressors)
                                    .readPreference(ReadPreference.secondaryPreferred())
                                    .build());
        }
        return mClient;
    }


    static int UPDATE_THREADS = 4;
    static int FIND_THREADS = 8;
    static int TOTAL_THREADS = UPDATE_THREADS+FIND_THREADS;

    @Override
    public void run(String... args) throws Exception {
        logger.info("Start");


        MongoCollection collection = ZlibCompressionApplication.getMongoClient().getDatabase("test").getCollection("products");
        collection.insertOne(new Document("foo", "bar"));
        logger.info("Inserted one doc.");

        Thread [] workThreads = new Thread[TOTAL_THREADS];

        for (int t=0; t<TOTAL_THREADS; ++t) {

            if ( (t % UPDATE_THREADS )== 0) {
                UpdateThread ut = new UpdateThread( 20000);
                workThreads[t] = new Thread(ut);
            }
            else {
                ReaderThread rt = new ReaderThread( 20000);
                workThreads[t] = new Thread(rt);
            }

            workThreads[t].start();
        }





        //getMongoClient().close();
        logger.info("Done");
    }
}
