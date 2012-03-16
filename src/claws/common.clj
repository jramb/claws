(ns ^{:doc "CLAWS common tools"
      :author "Jörg Ramb"}
    claws.common
    (:import [com.amazonaws.auth AWSCredentials PropertiesCredentials])
    (:require [clojure.string :as s])
  )

;;Note: (clojure.java.io/resource "AwsCredentials.properties")
;; returns URL

(defn default-credentials []
  "Returns the credentials stored in 'AwsCredentials.properties'."
  (PropertiesCredentials.
   (.getResourceAsStream (clojure.lang.RT/baseLoader)
                         "AwsCredentials.properties")))


(defn camelize
  ^{:doc "This camelizes a string."
    :test (fn []
            (assert (=
                     (camelize "this-is-a-world-to-love")
                     "ThisIsAWorldToLove")))}
  [string]
  (s/replace
   (str "-" string)
   #"[-_](\w)"
   (comp s/upper-case second)))


(defmacro set-with
  "Take the object and apply a series of options using '.withXXXX' on it."
  [object options]
  `(doto ~object
     ~@(for [[property value] options]
         (let [property (name property)
               setter   (str ".with" (camelize property)
                             #_(comment
                               (.toUpperCase (subs property 0 1))
                               (subs property 1)))]
           `(~(symbol setter) ~value)))))



