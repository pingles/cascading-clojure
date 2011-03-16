(defproject cascading-clojure "1.0.0-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :java-fork "true"
  :dependencies
    [[org.clojure/clojure "1.2.0"]
     [org.clojure/clojure-contrib "1.2.0"]
     [cascading "1.0.17-SNAPSHOT" :exclusions [javax.mail/mail janino/janino]]
     [clj-json "0.3.1"]
     [clj-serializer "0.1.1"]]
  :dev-dependencies
    [[lein-javac "1.2.1-SNAPSHOT"]]
  :aot
    [cascading.clojure.api
     cascading.clojure.testing])
