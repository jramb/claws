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
            DescribeTableRequest
            ResourceNotFoundException
            ]))

(defn inc-cu
  "Increments the clients consumed units by d"
  [client d]
  (swap! (:cu client) + d))


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
  (when i
    (into {}
          (map
           (fn [[k v]]
             {(keyword k) (un-av v)})
           i))))


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
      {:db client
       :cu (atom 0)})))

(defn shutdown [db]
  (.shutdown (:db db)))

(defn a-key
  "Constructs a Key object. The key value v is converted to an attribute value."
  [v]
  (set-with (Key.)
            {:hash-key-element (av v)}))

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
  [params]
  (let [ltr (ListTablesRequest.)]
    (when (contains? params :limit)
      (set-with ltr {:limit (Integer. (:limit params))}))
    (when (contains? params :start)
      (set-with ltr {:exclusive-start-table-name (:start params)}))
    ltr))

(defn list-tables
  [client & params]
  (seq (.getTableNames
        (.listTables (:db client) (list-tables-request params)))))

(defn put-item-request
  [table item]
  (set-with (PutItemRequest.)
            {:table-name table
             :item item}))


(defn put-item
  [client table item]
  (when-let [pir (.putItem (:db client) (put-item-request table (make-item item)))]
    (inc-cu client (.getConsumedCapacityUnits pir))
    (bean pir)))



(comment
  (def m {:tableName "bubbles"
          :hashKeyValue (av "Test2")})
  (set-with (QueryRequest.)
            {:table-name "bubbles"
             :hash-key-value (av "Test")}))


(defn query-request [table hkv & params]
  (set-with (QueryRequest.)
            {:table-name table
             :hash-key-value hkv}))


(defn get-item-request
  "Produces a GetItemRequest with some options:
:attributes can be a list of attributes to fetch (default is all).
:consistent-read enables consisten read."
  [table key params]
  (let [ir (GetItemRequest.)
        {:keys [attributes consistent-read]} params]
    (set-with ir
              {:table-name table
               :key key
               ;;:attributes-to-get (string-array "a" "b" "c")
               })
    (when attributes
      (set-with ir
                {:attributes-to-get
                 (collection-clojure-to-java
                  (map name attributes))}))
    (when consistent-read
      (set-with ir
                {:consistent-read consistent-read}))
    ir))



(defn get-item
  [client table kv & params]
  (when-let [gir (.getItem (:db client) (get-item-request table (a-key kv)
                                       (apply hash-map params)))
             ;; -> GetItemResult
             ]
    (inc-cu client (.getConsumedCapacityUnits gir))
    (to-map
     (.getItem gir))))


(defn describe-table
  [client table]
  (try
    (bean (.getTable
           (.describeTable (:db client)
                           (set-with (DescribeTableRequest.)
                                     {:table-name table}))))
    (catch ResourceNotFoundException e nil)))

