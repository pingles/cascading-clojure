(ns cascading.clojure.scheme-test
  (:use clojure.test)
  (:use cascading.clojure.testing)
  (:use cascading.clojure.io)
  (:require [clojure.contrib.java-utils :as ju])
  (:require [clojure.contrib.duck-streams :as ds])
  (:require [cascading.clojure.api :as c]))

; TODO: remove test duplication

(defn transform
  [input]
  [[(.toUpperCase (:name input)) (inc (get-in input [:age-data :age]))]])

(deftest json-line-test
  (with-log-level :warn
    (with-tmp-files [source (temp-dir "source")
                     sink   (temp-path "sink")]
      (let [lines [{:name "foo" :age-data {:age 23}}
                   {:name "bar" :age-data {:age 14}}]]
        (write-lines-in source "source.data" (map encode-json lines))
        (let [trans (-> (c/pipe "j")
                      (c/map #'transform
                        :< "input"
                        :fn> "output"))
              flow (c/flow
                     {"j" (c/lfs-tap (c/json-line "input") source)}
                     (c/lfs-tap (c/json-line) sink)
                     trans)]
         (c/exec flow)
         (is (= "[\"FOO\",24]\n[\"BAR\",15]\n"
                (ds/slurp* (ju/file sink "part-00000")))))))))

(deftest clojure-line-test
  (with-log-level :warn
    (with-tmp-files [source (temp-dir "source")
                     sink   (temp-path "sink")]
      (let [lines [{:name "foo" :age-data {:age 23}}
                   {:name "bar" :age-data {:age 14}}]]
        (write-lines-in source "source.data" (map pr-str lines))
        (let [trans (-> (c/pipe "j")
                      (c/map #'transform
                        :< "input"
                        :fn> "output"))
              flow (c/flow
                     {"j" (c/lfs-tap (c/clojure-line "input") source)}
                     (c/lfs-tap (c/clojure-line) sink)
                     trans)]
         (c/exec flow)
         (is (= "[\"FOO\" 24]\n[\"BAR\" 15]\n"
                (ds/slurp* (ju/file sink "part-00000")))))))))
