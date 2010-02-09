(ns cascading.clojure.flow-test-new
  (:use clojure.test
        clojure.contrib.java-utils
        cascading.clojure.io)
  (:import (cascading.tuple Fields)
           (cascading.pipe Pipe)
           (cascading.clojure Util ClojureMap))
  (:require (cascading.clojure [api :as c])))

(defn- deserialize-tuple [line]
  (read-string line))

(defn- serialize-tuple
  {:fields "line"}
  [tuple]
  (pr-str tuple))

(defn- serialize-vals
  {:fields "line"}
  [& vals]
  (pr-str (vec vals)))

(defn- line-sink-seq [tuple-entry-iterator]
  (map #(read-string (first (.getTuple %)))
       (iterator-seq tuple-entry-iterator)))

(defn- mash [f coll]
  (into {} (map f coll)))

(defn in-pipe [in-label in-fields]
  (-> (c/pipe in-label)
      (c/map [in-fields
	      #'deserialize-tuple])))

(defn in-pipes [fields-spec]
  (if (not (map? fields-spec))
    (in-pipe "in" fields-spec)
    (mash (fn [[in-label one-in-fields]]
	    [in-label (in-pipe in-label one-in-fields)])
	  fields-spec)))              

(defn in-tuples [tuples-spec]
  (if (map? tuples-spec) 
    tuples-spec
    {"in" tuples-spec}))

(defn- test-flow [in-pipes-spec in-tuples-spec assembler expected-out-tuples]
  (with-log-level :warn
    (with-tmp-files [source-dir-path (temp-dir  "source")
                     sink-path       (temp-path "sink")]
        (doseq [[in-label in-tuples] in-tuples-spec]
          (write-lines-in source-dir-path in-label
            (map serialize-tuple in-tuples)))
        (let [assembly   (-> in-pipes-spec
                           assembler
                           (c/map #'serialize-vals))
              source-tap-map (mash (fn [[in-label _]]
                                     [in-label
                                      (c/lfs-tap (c/text-line "line")
                                        (file source-dir-path in-label))])
                                   in-tuples-spec)
              sink-tap       (c/lfs-tap (c/text-line "line") sink-path)
              flow           (c/flow source-tap-map sink-tap assembly)
              out-tuples     (line-sink-seq (.openSink (c/exec flow)))]
          (is (= expected-out-tuples out-tuples))))))

(defn uppercase
  {:fields "upword"}
  [word]
  (.toUpperCase word))

(deftest map-test
  (test-flow
   (in-pipes "word")
   (in-tuples [["foo"] ["bar"]])
   (fn [in] (-> in (c/map #'uppercase)))
   [["FOO"] ["BAR"]]))

(defn extract-key
  {:fields "key"}
  [val]
  (second (re-find #".*\((.*)\).*" val)))

(deftest extract-test
  (test-flow
   (in-pipes ["val" "num"])
   (in-tuples [["foo(bar)bat" 1] ["biz(ban)hat" 2]])
   (fn [in] (-> in (c/map "val" #'extract-key ["key" "num"])))
   [["bar" 1] ["ban" 2]]))

(deftest inner-join-test
  (test-flow
   (in-pipes {"lhs" ["name" "num"]
	      "rhs" ["name" "num"]})
   (in-tuples {"lhs" [["foo" 5] ["bar" 6]]
	       "rhs" [["foo" 1] ["bar" 2]]})
   (fn [{lhs "lhs" rhs "rhs"}]
     (-> [lhs rhs]
	 (c/inner-join
          [["name"] ["name"]]
          ["name1" "val1" "nam2" "val2"])
	 (c/select ["val1" "val2"])))
   [[6 2] [5 1]]))

(deftest multi-pipe-inner-join-test
  (test-flow
   (in-pipes {"p1" ["name" "num"]
	      "p2" ["name" "num"]
	      "p3" ["name" "num"]})
   (in-tuples {"p1" [["foo" 5] ["bar" 6]]
	       "p2" [["foo" 1] ["bar" 2]]
	       "p3" [["foo" 7] ["bar" 8]]})
   (fn [{p1 "p1" p2 "p2" p3 "p3"}]
     (-> [p1 p2 p3]
	 (c/inner-join
          [["name"] ["name"] ["name"]]
          ["name1" "val1" "name2" "val2" "name3" "val3"])
	 (c/select ["val1" "val2" "val3"])))
   [[6 2 8] [5 1 7]]))

(deftest multi-pipe-multi-field-inner-join-test
  (test-flow
   (in-pipes {"p1" ["x" "y" "num"]
	      "p2" ["x" "y" "num"]
	      "p3" ["x" "y" "num"]})
   (in-tuples {"p1" [[0 1 5] [2 1 6]]
	       "p2" [[0 1 1] [2 1 2]]
	       "p3" [[2 1 7] [0 1 8]]})
   (fn [{p1 "p1" p2 "p2" p3 "p3"}]
     (-> [p1 p2 p3]
	 (c/inner-join
          [["x" "y"]["x" "y"]["x" "y"]]
          ["x1" "y1" "val1" "x2" "y2" "val2" "x3" "y3" "val3"])
	 (c/select ["val1" "val2" "val3"])))
   [[5 1 8] [6 2 7]]))