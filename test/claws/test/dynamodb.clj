(ns claws.test.dynamodb
  (:require [claws.dynamodb :as d])
  (:use [claws.common])
  (:use [clojure.test]))

(def test-time (java.util.Date.))

(deftest basic-eu
  ;; to use the EU instance, use:
  ;; (def dyndb (d/dynamodb-client :endpoint "dynamodb.eu-west-1.amazonaws.com"))
  (let [db (d/dynamodb-client :endpoint "dynamodb.eu-west-1.amazonaws.com")]
    (is db "We could open the EU instance")
    (.shutdown db)))


(deftest basic
  (let [db (d/dynamodb-client)]
    (is (d/list-tables db 10 nil) "Some tables exist")
    (let [test-table "Claws-Demo"
          id (str "Test-" test-time)
          test-record       {:Id id     ;the key is important
                             "a-number" (.getTime test-time)
                             "b-string" "Hej!!"
                             :c-num-set [4 5 6 7] ;lists are converted to sets!
                             "A strange key: works!" 5
                             :d-str-set ["Hipp" "Hopp" "Hej" "Jörg"]}
          reference-record  {:Id (:Id test-record)
                             :a-number (.getTime test-time)
                             :b-string "Hej!!"
                             :c-num-set #{4 5 6 7}        ;lists are converted to sets!
                             (keyword "A strange key: works!") 5
                             :d-str-set #{"Hipp" "Hopp" "Hej" "Jörg"}}]
      ;; First put the item into the table
      (d/put-item db test-table test-record)
      ;; Now fetch it back and see if it is as expected
      (let [r2 (d/get-item db test-table (:Id test-record))]
        ;; should be no surprises here
        (is (= r2 reference-record) "fetched record same as reference (expected) record")))
    (.shutdown db)))