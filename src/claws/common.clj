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




(defn camelize [string]
  (clojure.string/replace
   (str "-" string)
   #"[-_](\w)"
   (comp clojure.string/upper-case second)))
;;(camelize "this-is-a-world-to-love")

(defmacro set-with
  [object options]
  `(doto ~object
     ~@(for [[property value] options]
         (let [property (name property)
               setter   (str ".with" (camelize property)
                             #_(comment
                               (.toUpperCase (subs property 0 1))
                               (subs property 1)))]
           `(~(symbol setter) ~value)))))

