(ns cascading.clojure.buffer-test
  (:use clojure.test
        cascading.clojure.testing)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require [clojure.contrib.duck-streams :as ds]
            [clojure.contrib.java-utils :as ju]
            [cascading.clojure.api :as c]))

(defn- max-by [keyfn coll]
  (let [maxer (fn [max-elem next-elem]
                (if (> (keyfn max-elem) (keyfn next-elem))
                  max-elem
                  next-elem))]
    (reduce maxer coll)))

(defn maxbuff [elems emit]
  (emit (max-by second (iterator-seq elems))))

(deftest buffer-max-for-each-group
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 7] ["bar" 3] ["bar" 2] ["bat" 4]])
    (fn [in] (-> in
               (c/group-by "word")
               (c/buffer #'maxbuff)))
    [["bar" 3] ["bat" 7]]))

(defn maxpairs [elems emit]
  (let [elems-seq (iterator-seq elems)
	biggest (max-by second elems-seq)]
    (doseq [other (remove #(= % biggest) elems-seq)]
      (emit (concat other biggest)))))

(deftest buffer-max-and-pair
  (test-flow
    (in-pipes ["word" "subcount"])
    (in-tuples [["bar" 1] ["bat" 7] ["bar" 3] ["bar" 2] ["bat" 4]])
    (fn [in] (-> in
               (c/group-by "word")
               (c/buffer #'maxpairs
                 :fn> ["word" "subcount" "maxword" "maxsubcount"])))
    [["bar" 1 "bar" 3]
     ["bar" 2 "bar" 3]
     ["bat" 4 "bat" 7]]))
