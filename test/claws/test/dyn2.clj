(ns claws.test.dyn2
  (:require [claws.dynamodb :as d])
  (:use [claws.common])
  (:use [clojure.test]))

(def test-time (java.util.Date.))

(deftest range-tests
  (let [db (d/dynamodb-client)]
    ;; add a watch to se the CU ticking
    (add-watch (:cu db) 1
               (fn [key ref old new] (println "CU2 increased by " (- new old) " to " new)))
    ;; lets see if there are any tables
    (is (d/list-tables db :limit 10) "Some tables exist")
    (let [test-table "Claws-test"
          hash-key  (let [{:keys [year month day]} (bean test-time)]
                      (format "%s-%s-%s" year month day))
          range-key (.getTime test-time)
          test-record       {:Hash hash-key  ;together these must be unique, hash CAN be double
                             :Range range-key
                             "a-number" (.getTime test-time)
                             "b-string" "Hej!!"
                             :c-num-set [4 5 6 7] ;lists are converted to sets!
                             "A strange key: works!" 5
                             :d-str-set ["Hipp" "Hopp" "Hej" "Jörg"]}
          reference-record  {:Hash hash-key
                             :Range range-key
                             :a-number (.getTime test-time)
                             :b-string "Hej!!"
                             :c-num-set #{4 5 6 7}        ;lists are converted to sets!
                             (keyword "A strange key: works!") 5
                             :d-str-set #{"Hipp" "Hopp" "Hej" "Jörg"}}]
      ;; First put the item into the table
      (d/put-item db test-table test-record)
      ;; fill with 4 more items
      (dorun
       (for [i (range 1 5)]
         (d/put-item db test-table (update-in test-record [:Range] + i))))

      ;; Now fetch it back and see if it is as expected
      (let [r2 (d/get-item db test-table [hash-key range-key])]
        ;; should be no surprises here
        (is (= r2 reference-record) "fetched record same as reference (expected) record"))
      
      ;; Fetch key that is not there
      (is (nil? (d/get-item db test-table ["no such thing" 0])) "Fetching nonexisten key returns nil")
      
      ;; Query information about the table
      (is (d/describe-table db test-table) "could query information about the table")
      )
    (d/shutdown db)))
