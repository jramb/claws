;; ## Introduction
;;
;; This library is my shot at making a wrapper library for
;; some of the Amazon Web Services. To start with: it is NOT
;; complete. It is NOT ready for production. I have barely started
;; and expect major rewrites.

;; My current goal is to implement my personal needs to start with.
;; I scratch my own itches. This is work in progress for now, hopefully I
;; can reach a level where CLAWS could be useable for others as well.

(ns ^{:doc "CLAWS DynamoDB"
      :author "Jörg Ramb"}
  claws.dynamodb
  (:use [claws.common])
  (:import [java.util Collection])
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


;; ## Internal functions

(defn collection-clojure-to-java
  "Helper function to convert a clojure collection to an AWS Collection (needed for AttributeValues)"
  [collection]
  (java.util.Arrays/asList
   (object-array (apply hash-set (map str collection)))))

(defn collection-java-to-clojure
  "Helper function to convert an Java Collection to clojure"
  [^Collection jcoll]
  (into () jcoll))

;; ## Some basic functions

(defn av
  "Takes a clojure value and returns a new AttributeValue.
The type (number, string, number set, string set) is guessed."
  [v]
  (let [new-av (AttributeValue.)]
    (if (coll? v)
      (if (number? (first v))
        (.withNS new-av (collection-clojure-to-java v))   ;wants java.util.Collection
        (.withSS new-av (collection-clojure-to-java v)))
      (if (number? v)
        (.withN new-av (str v)) ;wants String as well...
        (.withS new-av (str v))))))

(defn un-av
  "Converts an AttributeValue back to a clojure string, number, set of strings och set of numbers,
depending on the type of the av."
  [av]
  (let [bav (bean av)]                  ;FIXME make this more elegant
    (cond
      (:n bav) (Long. (:n bav))
      (:s bav) (:s bav)
      (:SS bav) (apply hash-set
                       (collection-java-to-clojure (:SS bav)))
      (:NS bav) (apply hash-set
                       (map #(Long. %) (collection-java-to-clojure (:NS bav)))))))

(defn make-item
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
  (into {}
        (map
         (fn [[k v]]
           {(keyword k) (un-av v)})
         i)))


;; End of tools



;; ## Low level access
;; Now follows a basic level of encapsulation. Basically
;; the AWS APIs are wrapped in clojure friendly manner.

(defn dynamodb-client
  "Creates a DynamoDB client. Optional keyword parameters
are :credentials and :endpoing. Both have defaults.

For possible endpoints go to:
http://docs.amazonwebservices.com/general/latest/gr/rande.html"
  [& params]
  (let [{:keys [endpoint credentials]} params]
    (let [client (AmazonDynamoDBClient.
                  (or (:credentials params) (default-credentials)))]
      (when endpoint (.setEndpoint client endpoint))
      client)))

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
  (bean (.putItem client (put-item-request table (make-item item)))))



(comment
  (def m {:tableName "bubbles"
          :hashKeyValue (av "Test2")})
  (set-with (QueryRequest.)
            {:table-name "bubbles"
             :hash-key-value (av "Test")}))


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
  (make-item {:a 5, "b" "Hej" "c" [5, 6]})
  
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