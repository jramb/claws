(ns claws.test.dynamodb
  (:require [claws.dynamodb :as d])
  (:use [claws.common])
  (:use [clojure.test]))

(def test-time (java.util.Date.))

(comment
  (d/dynamodb-client :endpoint "dynamodb.eu-west-1.amazonaws.com")
  (d/list-tables-request {:limit (Integer. 10)}))

(deftest basic-eu
  ;; to use the EU instance, use:
  ;; (def dyndb (d/dynamodb-client :endpoint "dynamodb.eu-west-1.amazonaws.com"))
  (let [db (d/dynamodb-client :endpoint "dynamodb.eu-west-1.amazonaws.com")]
    (is db "We could open the EU instance")
    (d/shutdown db))
  (is (number? (invoke-instance-method-m "getTime" test-time)) "invoke-instance-method-m works"))


(deftest basic
  (let [db (d/dynamodb-client)]
    (add-watch (:cu db) 1
               (fn [key ref old new] (println "CU increased by " (- new old) " to " new)))
    (is (d/list-tables db :limit 10) "Some tables exist")
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
        (is (= r2 reference-record) "fetched record same as reference (expected) record"))
      (is (nil? (d/get-item db test-table "no such key")) "Fetching nonexisten key returns nil")
      
      ;; Fetch only some columns
      (let [only-attrib [:a-number :c-num-set]
            r2 (d/get-item db test-table (:Id test-record)
                           :attributes only-attrib)]
        (is (= r2 (select-keys reference-record only-attrib))
            "fetched record same as reference record (only requested attribs)"))

      ;; Query information about the table
      (is (d/describe-table db test-table) "could query information about the table")
      (is (nil? (d/describe-table db "no-such")) "non-existent table described as nil")
      )
    (d/shutdown db)))