/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
@Author : Ayush Chaudhary
Email: ayush.shyam@gmail.com
 */
package com.mycompany.mongoclientconnectionexample;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoClientConnectionExample {

    public static void main(String[] args) {
        // Connection String to your MongoDB Database (replace with yours)
        String connectionString = "mongodb+srv://<USE_YOUR_DB_CONNECTIONSTRING>/?retryWrites=true&w=majority&appName=DBCluster";

        // Define the server API version (usually V1)
        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();

        // Build the MongoClient settings with connection string and server API        
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .serverApi(serverApi)
                .build();

        // Create a new client and connect to the server
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            try {
                // Get a reference to the database named "firstMongoDB"
                MongoDatabase database = mongoClient.getDatabase("firstMongoDB");
                // **Commented out methods for reference:**

                // To delete a collection (use with caution!)
                // deleteCollection(database, "Student");
                // To create a new collection
                // createCollection(database, "Student");
                // To insert a student document (replace with your data)
                // insertStudent(database, "Student", "Vivian Chaudhary", 48, "Kindergarten");
                // To retrieve all students sorted by roll number (replace with your schema)
                // getAllStudentSortedByRollNumber(database, "Student");
                // To update a student document (replace with your data)
                // updateStudent(database, "Student", 18, "First Class");
                // **Your desired method - filter students by name containing a string**
                getStudentFliterByStringInName(database, "Student", "Viv");

            } catch (MongoException e) {
                e.printStackTrace();
            }
        }
    }

    // Method to delete a collection (use with caution!)
    public static void deleteCollection(MongoDatabase database, String collectionName) {
        // Drop the collection
        database.getCollection(collectionName).drop();
        System.out.println("Collection deleted successfully");
    }

    // Method to insert a document into a collection
    public static void insertStudent(MongoDatabase database, String collectionName, String name, int rollNumber, String Class) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);
            // Create a new document that conforms to the schema
            Document newDocument = new Document()
                    .append("name", name)
                    .append("rollnumber", rollNumber)
                    .append("class", Class);

            // Insert the document into the collection
            collection.insertOne(newDocument);

            System.out.println("Document inserted successfully.");
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    // Method to retrieve documents from a collection (replace with your schema)
    public static void getStudentByRollNumber(MongoDatabase database, String collectionName, int rollNumber) {
        try {
            MongoCollection<Document> collection = database.getCollection("Ayush");
            // Define the filter to retrieve the document where name = "Jane Doe"
            Document student = new Document("rollnumber", rollNumber);
            // Fetch the document
            Document result = collection.find(student).first();
            //Document result = collection.find(and(eq("email","ayush.shyam@gmail.com"),lte("age", 36))).first();
            if (result != null) {
                System.out.println("Result " + result.toString());
            } else {
                System.out.println("No student found with roll number " + rollNumber);
            }
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    // Method to update a document in a collection
    public static void updateStudent(MongoDatabase database, String collectionName, int rollNumber, String className) {
        MongoCollection<Document> collection = database.getCollection(collectionName);

        //Set the condition
        Bson filter = Filters.eq("rollnumber", rollNumber);

        // Specify the update operation
        Bson update = Updates.set("class", className);  // Set the new class

        // Perform the update
        UpdateResult result = collection.updateOne(filter, update);
        if (result.getMatchedCount() > 0) {
            System.out.println("Document matched the filter.");
            if (result.getModifiedCount() > 0) {
                System.out.println("Document was updated successfully.");
            } else {
                System.out.println("Document matched but no changes were made.");
            }
        } else {
            System.out.println("No document matched the filter.");
        }
    }

    // Method to filter students by a string contained in their names
    public static void getStudentFliterByStringInName(MongoDatabase database, String collectionName, String filter) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);

            //Case sensitive filter
            Bson namefilter = regex("name", filter);
            Document sort = new Document("rollnumber", 1);

            MongoCursor<Document> cursor = collection.find(namefilter).sort(sort).iterator();
            if (cursor != null) {
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } else {
                System.out.println("No student found");
            }
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    public static void getAllStudentSortedByRollNumber(MongoDatabase database, String collectionName) {
        try {
            MongoCollection<Document> collection = database.getCollection(collectionName);
            // Sort by rollnumber in ascending order
            Document sort = new Document("rollnumber", -1); // Use -1 for descending order
            MongoCursor<Document> cursor = collection.find().sort(sort).iterator();

            //Document result = collection.find(and(eq("email","ayush.shyam@gmail.com"),lte("age", 36))).first();
            if (cursor != null) {
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } else {
                System.out.println("No student found");
            }
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    // Method to create a new collection
    public static void createCollection(MongoDatabase database, String collectionName) {
        try {

            //Create a new collection
            Document studentSchema = new Document("$jsonSchema", new Document()
                    .append("bsonType", "object")
                    .append("required", Arrays.asList("name", "rollnumber", "class"))
                    .append("properties", new Document()
                            .append("name", new Document()
                                    .append("bsonType", "string")
                                    .append("description", "must be a string and is required"))
                            .append("rollnumber", new Document()
                                    .append("bsonType", "int")
                                    .append("description", "must be an integer and is required"))
                            .append("class", new Document()
                                    .append("bsonType", "string")
                                    .append("description", "must be a string and is required"))
                    ));

            // Create the options object with the validator
            ValidationOptions validationOptions = new ValidationOptions()
                    .validator(studentSchema);

            // Create the collection options with schema validation
            CreateCollectionOptions options = new CreateCollectionOptions().validationOptions(validationOptions);

            // Create the collection with schema validation
            database.createCollection(collectionName, options);
            System.out.println("Collection created successfully");
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

}
