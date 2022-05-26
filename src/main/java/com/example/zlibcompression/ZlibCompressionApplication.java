package com.example.zlibcompression;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.MongoCompressor.createZlibCompressor;

@SpringBootApplication
public class ZlibCompressionApplication implements CommandLineRunner {

    private  static Logger logger = LogManager.getLogger(ZlibCompressionApplication.class);

    public static void main(String[] args) {

        /* We are allowing invalidHostnames, so we do not need to identify our host
        System.setProperty("javax.net.ssl.keyStore", "/etc/ssl/keystore.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "P4ssw0rd");
        System.setProperty("javax.net.ssl.keyStoreType", "JKS");
        */

        System.setProperty("javax.net.ssl.trustStore", "/etc/ssl/truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "P4ssw0rd");
        System.setProperty("javax.net.ssl.trustStoreType", "JKS");


        SpringApplication.run(ZlibCompressionApplication.class, args);
    }

    private static MongoClient mClient = null;


    public static MongoClient getMongoClient() {
        if (mClient == null) {

            //
            ServerAddress [] replicaSet = new ServerAddress[3];
            replicaSet[0] = new ServerAddress("m04.mdbkrb5.net", 27017);
            replicaSet[1] = new ServerAddress("m05.mdbkrb5.net", 27017);
            replicaSet[2] = new ServerAddress("m06.mdbkrb5.net", 27017);

            String user = "root" ;
            String authDb ="admin";
            String pwd = "P4ssw0rd";


            MongoCredential credentials = MongoCredential.createScramSha256Credential(user, authDb, pwd.toCharArray());

            List<MongoCompressor> compressors = new ArrayList<>();
            compressors.add(createZlibCompressor());
            mClient  = MongoClients.create(
                            MongoClientSettings.builder()
                                    .applyToClusterSettings(builder ->
                                        builder.hosts(Arrays.asList(replicaSet)))
                                    .credential(credentials)
                                    .compressorList( compressors)
                                    .applyToSslSettings(builder ->
                                        builder.enabled(true))
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



        MongoCollection collection = ZlibCompressionApplication.getMongoClient().getDatabase("test").getCollection("bleh");
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
