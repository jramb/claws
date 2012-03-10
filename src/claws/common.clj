(ns ^{:doc "CLAWS common tools"
      :author "Jörg Ramb"}
    claws.common
  (:import [com.amazonaws.auth AWSCredentials PropertiesCredentials])
  )

;;(clojure.java.io/resource "awscredentials.properties")

(defn default-credentials []
  (PropertiesCredentials.
   (.getResourceAsStream (clojure.lang.RT/baseLoader)
                         "AwsCredentials.properties")))


