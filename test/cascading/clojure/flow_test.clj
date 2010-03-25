(ns cascading.clojure.flow-test
  (:use clojure.test
        clojure.contrib.java-utils
        cascading.clojure.testing
        cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require [clojure.contrib.duck-streams :as ds])
  (:require [clojure.contrib.java-utils :as ju])
  (:require [cascading.clojure.api :as c]))

(defn uppercase
  [word]
  (.toUpperCase word))

(deftest map-test
  (test-flow
   (in-pipes "word")
   (in-tuples [["foo"] ["bar"]])
   (fn [in] (-> in (c/map #'uppercase)))
   [["FOO"] ["BAR"]]))

(deftest map-without-defaults
  (test-flow
   (in-pipes ["x" "y" "foo"])
   (in-tuples [[2 3 "blah"] [7 3 "blah"]])
   (fn [in] (-> in (c/map #'+ :< ["x" "y"] :fn> "sum" :> "sum")))
   [[5] [10]]))

(defn extract-key
  [val]
  (second (re-find #".*\((.*)\).*" val)))

(deftest extract-test
  (test-flow
   (in-pipes ["val" "num"])
   (in-tuples [["foo(bar)bat" 1] ["biz(ban)hat" 2]])
   (fn [in] (-> in (c/map #'extract-key :< "val" :fn> "key" :> ["key" "num"])))
   [["bar" 1] ["ban" 2]]))

(def sum (c/agg + 0))

(deftest aggreate-test
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 2] ["bar" 3] ["bar" 2] ["bat" 1]])
    (fn [in] (-> in
               (c/group-by "word")
               (c/aggregate #'sum :< "subcount" :fn> "count" :> ["word" "count"])))
    [["bar" 6] ["bat" 3]]))
