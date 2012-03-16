(ns ^{:doc "Demo of CLAWS"}
  demo.space
  (:require [claws.dynamodb :as d]))


;; create a client with all defaults (reads credentials from property)
(def dyndb (d/dynamodb-client))

(comment
  ;; to use the EU instance:
  (def dyndb (d/dynamodb-client :endpoint "dynamodb.eu-west-1.amazonaws.com"))
  (def dyndb (d/dynamodb-client
              :endpoint "dynamodb.eu-west-1.amazonaws.com"
              :credentials (claws.common/default-credentials)))
  )


;; list all tables, starting from beginning (nil), limit list to 10
(d/list-tables dyndb 10 nil)

;; the table we work with
(def tableName "Claws-Demo")

;; some example record (a map). This must contain the table key, of course.
;; The keys can be either keywords (recommended) or strings.
;; The fetching functions will always return keyword style.
(def my-record
  {:Id "Test2"                          ;the key is important
   "a-number" 99
   "b-string" "Hej!!"
   :c-num-set [4 5 6 7]                 ;lists are converted to sets!
   ;;event this would work: "A strange key: works!" 5
   :d-str-set ["Hipp" "Hopp" "Hej" "Jörg"]})


;; put (store or update) my-record into the table
(d/put-item dyndb tableName my-record)

;; and fetch it back:
(d/get-item dyndb tableName "Test2")
(comment
  --> 
  {:b-string  "Hej!!",
   :a-number  99,
   :Id        "Test2",
   :d-str-set #{"Jörg" "Hipp" "Hopp" "Hej"},
   :c-num-set #{4 5 6 7}}
  )
  ;;
  ;; Note that the keys now are normalized to keywords
  ;; and the values are either string, number, *set* of strings or *set*
  ;; of numbers.

;; Shut down the client (probably optional)
(.shutdown dyndb)