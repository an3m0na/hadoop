package org.apache.hadoop.tools.posum.database;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.tools.posum.POSUMConfiguration;
import org.bson.Document;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Created by ane on 3/1/16.
 */
public class MongoConnector {

    protected MongoDatabase db;
    protected MongoClient client;

    public MongoConnector(String databaseName){
        this(databaseName, null);
    }

    public MongoConnector(String databaseName, String databaseUrl) {
        if (databaseName == null)
            databaseName = "test";
        if (databaseUrl != null)
            client = new MongoClient(databaseUrl);
        client = new MongoClient();
        db = client.getDatabase(databaseName);
    }

    public void insertDoc(String collection, Document doc) {
        db.getCollection(collection).insertOne(doc);
    }

    public void insertDocs(String collection, List<Document> docs) {
        db.getCollection(collection).insertMany(docs);
    }

    public void deleteDocs(String collection, Document queryDoc) {
        db.getCollection(collection).deleteMany(queryDoc);
    }

    public void deleteDoc(String collection, Document queryDoc) {
        db.getCollection(collection).findOneAndDelete(queryDoc);
    }

    public void updateDoc(String collection, Document queryDoc, Document setterDoc) {
        db.getCollection(collection).updateOne(queryDoc, setterDoc);
    }

    public void updateDocs(String collection, Document queryDoc, Document setterDoc) {
        db.getCollection(collection).updateMany(queryDoc, setterDoc);
    }

    public void replaceDoc(String collection, Document queryDoc, Document newDoc) {
        db.getCollection(collection).replaceOne(queryDoc, newDoc);
    }

    public AggregateIterable<Document> aggregateDocs(String collection, Document... query) {
        List<Document> actualQuery = query == null ? Collections.<Document>emptyList() : Arrays.asList(query);
        return db.getCollection(collection).aggregate(actualQuery);
    }


    public FindIterable<Document> findDocs(String collection, Document queryDoc) {
        if (queryDoc == null)
            queryDoc = new Document();
        return db.getCollection(collection).find(queryDoc);
    }

}
