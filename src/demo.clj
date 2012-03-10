(ns your-ns
  (:require [claws.dynamodb :as d]))

(def dyndb (d/dynamodb-client))
; to use the EU instance, but the default credentials (via file)
;;(def dyndb (d/dynamodb-client nil "dynamodb.eu-west-1.amazonaws.com"))


(def tableName "bubbles")

(def my-record
  {:Domain-UUID "Test2"
   "a" 99 "b" "Hej!!" :c [4 5 6 7]
   :d ["Hipp" "Hopp" "Hej" "Jörg"]})

(d/put-item dyndb tableName my-record)

(d/get-item dyndb tableName "Test2")