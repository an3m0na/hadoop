package org.apache.hadoop.tools.posum.database.store;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Created by ane on 3/1/16.
 */
public class MongoConnector {

    protected MongoClient client;


    public MongoConnector(String databaseUrl) {
        if (databaseUrl != null)
            client = new MongoClient(databaseUrl);
        client = new MongoClient();
    }

    public void insertDoc(String database, String collection, Document doc) {
        client.getDatabase(database).getCollection(collection).insertOne(doc);
    }

    public void insertDocs(String database, String collection, List<Document> docs) {
        client.getDatabase(database).getCollection(collection).insertMany(docs);
    }

    public void deleteDocs(String database, String collection, Document queryDoc) {
        client.getDatabase(database).getCollection(collection).deleteMany(queryDoc);
    }

    public void deleteDoc(String database, String collection, Document queryDoc) {
        client.getDatabase(database).getCollection(collection).findOneAndDelete(queryDoc);
    }

    public void updateDoc(String database, String collection, Document queryDoc, Document setterDoc) {
        client.getDatabase(database).getCollection(collection).updateOne(queryDoc, setterDoc);
    }

    public void updateDocs(String database, String collection, Document queryDoc, Document setterDoc) {
        client.getDatabase(database).getCollection(collection).updateMany(queryDoc, setterDoc);
    }

    public void replaceDoc(String database, String collection, Document queryDoc, Document newDoc) {
        client.getDatabase(database).getCollection(collection).replaceOne(queryDoc, newDoc);
    }

    public AggregateIterable<Document> aggregateDocs(String database, String collection, Document... query) {
        List<Document> actualQuery = query == null ? Collections.<Document>emptyList() : Arrays.asList(query);
        return client.getDatabase(database).getCollection(collection).aggregate(actualQuery);
    }

    public FindIterable<Document> findDocs(String database, String collection, Document queryDoc) {
        if (queryDoc == null)
            queryDoc = new Document();
        return client.getDatabase(database).getCollection(collection).find(queryDoc);
    }

}
