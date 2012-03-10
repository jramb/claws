(ns ^{:doc "CLAWS DynamoDB"
      :author "Jörg Ramb"}
  claws.dynamodb
  (:use [claws.common])
  (:import [com.amazonaws AmazonClientException AmazonServiceException])
  (:import [com.amazonaws.services.dynamodb AmazonDynamoDBClient])
  (:import [com.amazonaws.services.dynamodb.model
            AttributeValue
            ComparisonOperator
            Condition
            CreateTableRequest
            DescribeTableRequest
            Key
            KeySchema
            KeySchemaElement
            ProvisionedThroughput
            PutItemRequest
            PutItemResult
            ScanRequest
            ScanResult
            TableDescription
            TableStatus
            UpdateTableRequest
            ListTablesRequest
            GetItemRequest
            ]))



;; http://docs.amazonwebservices.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/dynamodb/AmazonDynamoDBClient.html
;; endpoints: http://docs.amazonwebservices.com/general/latest/gr/rande.html

(defn dynamodb-client
  "Creates a DynamoDB client.
Called: (dynamodb-client [credentials [endpoint]]).
Uses default credentials if none given and default endpoint."
  ([]
     (dynamodb-client (default-credentials)))
  ([credentials]
     (AmazonDynamoDBClient. (or credentials (default-credentials))))
  ([credentials endpoint] 
     (doto (AmazonDynamoDBClient. (or credentials (default-credentials)))
       (.setEndpoint endpoint))))

(defn throughput [r w]
  (doto (ProvisionedThroughput.)
    (.withReadCapacityUnits (long r))
    (.withWriteCapacityUnits (long w))))

(defn update-table-request [table-name]
  (doto (UpdateTableRequest.)
    (.withTableName table-name)))

(defn update-table-throughput [table-name r w]
  (doto (update-table-request table-name)
    (.withProvisionedThroughput (throughput r w))))

#_(defmacro with->
  "Like -> but special for the AWS '.withXXXX' forms for no parameter constructors.
The first parameter is the class, the following xxxx are constructed as .withxxxx"
  ([x] x)
  ([x form] (if (seq? form)
              (with-meta `(~(first form) ~x ~@(next form)) (meta form))
              (list form x)))
  ([x form & more] `(-> (-> ~x ~form) ~@more)))
#_(with-> ListTablesRequest
  (limit (Integer. limit))
  (exclusive-start-table-name start-with))
;;->

(defn list-tables-request
  [limit start-with]
  (-> (ListTablesRequest.)
    (.withLimit (Integer. limit))       ;why does (type (int 10)) ;=> java.lang.Long??
    (.withExclusiveStartTableName start-with)))

(defn list-tables
  [client limit start-with]
  (seq (.getTableNames
        (.listTables client (list-tables-request limit start-with)))))

(defn put-item-request
  [table item]
  (-> (PutItemRequest.)
      (.withTableName table)
      (.withItem item)))


(defn to-attribute-value
  [v]
  (let [av (AttributeValue.)]
    (if (seq? v)
      (if (number? (first v))
        (.withNS av (object-array v))   ;wants java.util.Collection
        (.withSS av (object-array (map str v))))
      (if (number? v)
        (.withN av (str v)) ;wants String as well...
        (.withS av (str v))))))

(defn to-item
  "Convert Clojure map to AWS Item."
  [m]
  (java.util.HashMap.
   (apply conj
          (map (fn [[k v]]
                 {(if (keyword? k) (name k) k)
                  (to-attribute-value v)}) m))))

(defn to-map
  "Convert AWS style item to good Clojure"
  [i]
  (into {} i))

(defn put-item
  [client table item]
  (bean (.putItem client (put-item-request table (to-item item)))))

(defn get-item-request
  [table key]
  (-> (GetItemRequest.)
      (.withTableName table)
      (.withKey key)
      ;;(.withAttributesToGet (string-array "a" "b" "c"))
      ))

(defn a-key [v]
  (-> (Key.)
      (.withHashKeyElement (to-attribute-value v))))

(defn get-item
  [client table kv]
  (to-map
   (.getItem
    (.getItem client (get-item-request table (a-key kv))))))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(comment
  
  (def dyndb (dynamodb-client nil "dynamodb.eu-west-1.amazonaws.com"))
  (def tableName "bubbles")
  (def credentials default-credentials)
  (.updateTable dyndb (update-table-throughput tableName 5 5))
  (list-tables dyndb 10 nil)
  (to-item {:a 5, "b" "Hej" "c" [5, 6]})

  ;; this stores an item. The hash-is the only important thing
  (put-item dyndb tableName {:Domain-UUID "Test"
                             "a" 99 "b" "Hej!!" :c [4 5 6 7]
                             :d ["Hipp" "Hopp" "Hej" "Jörg"]})

  (get-item dyndb tableName "Test")





  ;;   // Create a table with a primary key named 'name', which holds a string
  ;; CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
  ;;     .withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName("name").withAttributeType("S")))
  ;;     .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L));
  ;; TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
  ;; System.out.println("Created Table: " + createdTableDescription);

  ;; // Wait for it to become active
  ;; waitForTableToBecomeAvailable(tableName);


  #_(def createTableRequest
      (doto (CreateTableRequest.)
        (.withTableName tableName)
        (.withKeySchema (KeySchema.
                         (doto (KeySchemaElement.)
                           (.withAttributeName "name")
                           (.withAttributeType "S"))))
        (.withProvisionedThroughput (doto (ProvisionedThroughput.)
                                      (.withReadCapacityUnits (long 10))
                                      (.withWriteCapacityUnits (long 10))))))

  (.getTableDescription (.createTable dyndb createTableRequest))
  (.getTableDescription )

  ;; CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
  ;;     .withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName("name").withAttributeType("S")))
  ;;     .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L));
  ;; TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();



  (.describeTable
   dynamoDB
   (doto (DescribeTableRequest.)
     (.withTableName "bubbles")))

)