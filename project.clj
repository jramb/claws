(defproject claws "0.0.2-SNAPSHOT"
  :description "Clojure wrapper for the Amazon AWS services."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [com.amazonaws/aws-java-sdk "1.3.3"]]
  :dev-dependencies [;;lein plugin install lein-clojars 0.8.0
                     ;;note to self: to publish: lein push
                     [lein-marginalia "0.7.0"]]
  ;; Files with names matching any of these patterns will be excluded from jars
  :jar-exclusions [#"(?:^|/).svn/" #"AwsCredentials.properties"]
  )