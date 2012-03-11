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
            QueryRequest
            ]))


(defn av
  "Converts a value to an AttributeValue, guessing the type"
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
  "Convert Clojure map to a plain old HashMap (used for AWS Item)."
  [m]
  (java.util.HashMap.
   (apply conj
          (map (fn [[k v]]
                 {(if (keyword? k) (name k) k)
                  (av v)}) m))))

(defn to-map
  "Convert plain old HashMap (AWS style item) to good Clojure"
  [i]
  (into {} i))




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

(defn throughput
  "Creates a ProvisionedThroughput "
  [r w]
  (set-with (ProvisionedThroughput.)
            {:read-capacity-units (long r)
             :write-capacity-units (long r)}))


(defn update-table-request [table-name]
  (set-with (UpdateTableRequest.)
            {:table-name table-name}))

(defn update-table-throughput [table-name r w]
  (set-with (update-table-request table-name)
            {:provisioned-throughput (throughput r w)}))


(defn list-tables-request
  [limit start-with]
  (set-with (ListTablesRequest.)
            {:limit (Integer. limit) ;why does (type (int 10)) ;=> java.lang.Long??
             :exclusive-start-table-name start-with}))

(defn list-tables
  [client limit start-with]
  (seq (.getTableNames
        (.listTables client (list-tables-request limit start-with)))))

(defn put-item-request
  [table item]
  (set-with (PutItemRequest.)
            {:table-name table
             :item item}))


(defn put-item
  [client table item]
  (bean (.putItem client (put-item-request table (to-item item)))))

(comment the API is modelled for .NET as it seems... see if we can beat that (in terms of elegant programmability)
;;          var request = new QueryRequest
;; {
;;   TableName = "Reply",
;;   HashKeyValue = new AttributeValue { S = "Amazon DynamoDB#DynamoDB Thread 1" }
;; };

;; var response = client.Query(request);
;; var result = response.QueryResult;

;; foreach (Dictionary<string, AttributeValue> item in response.QueryResult.Items)
;; {
;;   // Process the result.
;;   PrintItem(item);
;;  }
         )

;; m could be
(comment m could be
         {:TableName "bubbles"
          :HashKeyValue (av "Test2")})

#_(let [o (QueryRequest.)]
  (. o withTableName "bubbles")
  (. o withHashKeyValue (av "Test2")))

#_(defn make-with
  "Construct a s-expression to be used in doto"
  [[setter & parms]]
  `(~(symbol (str ".with" (name setter))) ~@parms))
;;(make-with [:Fire 10 22]) -> (.withFire 10 22)
#_(defmacro map-with [class things]
  `(doto ~class ~@(map make-with things)))



;;
(def m {:tableName "bubbles"
        :hashKeyValue (av "Test2")})
(wither (QueryRequest.)
        {:table-name "bubbles"
         :hash-key-value (av "Test")})


(defn query-request [table hkv]
  (set-with (QueryRequest.)
            {:table-name table
             :hash-key-value hkv}))


(defn get-item-request
  [table key]
  (set-with (GetItemRequest.)
            {:table-name table
             :key key
             ;;:attributes-to-get (string-array "a" "b" "c")
             }))

(defn a-key [v]
  (set-with (Key.)
            {:hash-key-element (av v)}))

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
  
  (query-request "bubbles"  (av "Test2"))

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