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
  (let [cred (.getResourceAsStream (clojure.lang.RT/baseLoader)
                                   "AwsCredentials.properties")]
    (when-not cred
      (throw (Exception. "AwsCredentials.properties not found in classpath")))
    (PropertiesCredentials. cred)))


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


;; works only if the method can be passed as literal
(defmacro invoke-instance-method-m
  "Invoke the literal(!) string method on the object as a method, passing in optional params."
  [method object & params]
  `(. ~object ~(symbol method) ~@params))


;; works even with non literals in the method, but is slower!
(defn invoke-instance-method
  [method object & params]
  (clojure.lang.Reflector/invokeInstanceMethod
   object method (into-array Object params)))

(comment
  ;; 11 ms:
  (time
   (let [d (java.util.Date.)]
     (dotimes [_ 500000]
       (invoke-instance-method-m "getTime" d))))
  ;; 1272 ms:
  (time
   (let [d (java.util.Date.)]
     (dotimes [_ 500000]
       (invoke-instance-method "getTime" d))))

  (let [d (java.util.Date.)
        m "setMonth"]
    (invoke-instance-method-m m d 11)
    d))


(defmacro set-with
  "Take the object and apply a series of options using '.withXXXX' on it."
  [object options]
  `(doto ~object
     ~@(for [[property value] options]
         (let [property (name property)
               setter   (str ".with" (camelize property))]
           `(~(symbol setter) ~value)))))

(defn set-with-x
  "Take the object and apply a series of options using '.withXXXX' on it."
  [object options]
  (if object
    (dorun (for [[property value] options]
             (let [property (name property)
                   setter   (str "with" (camelize property))]
               (invoke-instance-method setter object value)))))
  object)



