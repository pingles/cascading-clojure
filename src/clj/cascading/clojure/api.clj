(ns cascading.clojure.api
  (:refer-clojure :exclude (count first filter mapcat map))
  (:use [clojure.contrib.seq-utils :only [find-first indexed]])
  (:require [clj-json :as json])
  (:import (cascading.tuple Tuple TupleEntry Fields)
           (cascading.scheme TextLine SequenceFile)
           (cascading.flow Flow FlowConnector)
           (cascading.cascade Cascades)
           (cascading.operation Identity)
           (cascading.operation.regex RegexGenerator RegexFilter)
           (cascading.operation.aggregator First Count)
           (cascading.pipe Pipe Each Every GroupBy CoGroup)
           (cascading.pipe.cogroup InnerJoin OuterJoin LeftJoin RightJoin)
           (cascading.scheme Scheme)
           (cascading.tap Hfs Lfs Tap)
           (org.apache.hadoop.io Text)
           (org.apache.hadoop.mapred TextInputFormat TextOutputFormat
                                     OutputCollector JobConf)
           (java.util Properties Map UUID)
           (cascading.clojure ClojureFilter ClojureMapcat ClojureMap
                              ClojureAggregator Util ClojureBuffer)
           (clojure.lang Var)
           (java.io File)
           (java.lang RuntimeException Comparable)))

(defn ns-fn-name-pair [v]
  (let [m (meta v)]
    [(str (:ns m)) (str (:name m))]))

(defn fn-spec [v-or-coll]
  "v-or-coll => var or [var & params]
   Returns an Object array that is used to represent a Clojure function.
   If the argument is a var, the array represents that function.
   If the argument is a coll, the array represents the function returned
   by applying the first element, which should be a var, to the rest of the
   elements."
  (cond
    (var? v-or-coll)
      (into-array Object (ns-fn-name-pair v-or-coll))
    (coll? v-or-coll)
      (into-array Object
        (concat
          (ns-fn-name-pair (clojure.core/first v-or-coll))
          (next v-or-coll)))
    :else
      (throw (IllegalArgumentException. (str v-or-coll)))))

(defn- collectify [obj]
  (if (sequential? obj) obj [obj]))

(defn fields
  {:tag Fields}
  [obj]
  (if (or (nil? obj) (instance? Fields obj))
    obj
    (Fields. (into-array String (collectify obj)))))

(defn fields-array
  [fields-seq]
  (into-array Fields (clojure.core/map fields fields-seq)))

(defn pipes-array
  [pipes]
  (into-array Pipe pipes))

(defn- fields-obj? [obj]
  "Returns true for a Fileds instance, a string, or an array of strings."
  (or
    (instance? Fields obj)
    (string? obj)
    (and (sequential? obj) (every? string? obj))))


; in-fields: subset of fields from incoming pipe that are passed to function
;   defaults to all
; func-fields: fields declared to be returned by the function
;   must be given in meta data or as [override-fields #'func-var]
;   no default, error if missing
; out-fields: subset of (union in-fields func-fields) that flow out of the pipe
  ;   defaults to func-fields

(defn parse-args
  "
  arr => func-spec in-fields? :fn> func-fields :> out-fields
  
  returns [in-fields func-fields spec out-fields]
  "
  ([arr] (parse-args arr Fields/RESULTS))
  ([arr defaultout]
     (let
       [func-args           (clojure.core/first arr)
        varargs             (rest arr)
        spec                (fn-spec func-args)
        func-var            (if (var? func-args) func-args (clojure.core/first func-args))
                              first-elem (clojure.core/first varargs)
        [in-fields keyargs] (if (or (nil? first-elem)
                                    (keyword? first-elem))
                                  [Fields/ALL (apply hash-map varargs)]
                                  [(fields (clojure.core/first varargs))
                                   (apply hash-map (rest varargs))])
        options             (merge {:fn> (:fields (meta func-var)) :> defaultout} keyargs)
        result              [in-fields (fields (:fn> options)) spec (fields (:> options))]]

        result )))


(defn- uuid []
  (str (UUID/randomUUID)))

(defn pipe
  "Returns a Pipe of the given name, or if one is not supplied with a
   unique random name."
  ([]
   (Pipe. (uuid)))
  ([#^String name]
   (Pipe. name)))

(defn- as-pipes
  [pipe-or-pipes]
  (let [pipes (if (instance? Pipe pipe-or-pipes)
[pipe-or-pipes] pipe-or-pipes)]
  (into-array Pipe pipes)))

(defn filter [& args]
  (fn [previous]
    (let [[in-fields _ spec _] (parse-args args)]
      (Each. previous in-fields
        (ClojureFilter. spec)))))

(defn mapcat [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMapcat. func-fields spec) out-fields))))

(defn map [& args]
  (fn [previous]
    (let [[in-fields func-fields spec out-fields] (parse-args args)]
    (Each. previous in-fields
      (ClojureMap. func-fields spec) out-fields))))

(defn group-by
  ([group-fields]
    (fn [& previous] (GroupBy. (as-pipes previous) (fields group-fields))))
  ([group-fields sort-fields]
    (fn [& previous] (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields))))
  ([group-fields sort-fields reverse-order]
    (fn [& previous] (GroupBy. (as-pipes previous) (fields group-fields) (fields sort-fields) reverse-order))))

(defn count [#^String count-fields]
  (fn [previous]
    (Every. previous (Count. (fields count-fields)))))

(defn first []
  (fn [previous]
    (Every. previous (First.))))

(defn select [keep-fields]
  (fn [previous]
    (Each. previous (fields keep-fields) (Identity.))))

(defn raw-each
  ([arg1] (fn [p] (Each. p arg1)))
  ([arg1 arg2] (fn [p] (Each. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p] (Each. p arg1 arg2 arg3))))

(defn raw-every
  ([arg1] (fn [p] (Every. p arg1)))
  ([arg1 arg2] (fn [p] (Every. p arg1 arg2)))
  ([arg1 arg2 arg3] (fn [p] (Every. p arg1 arg2 arg3))))

;; we shouldn't need a seq for fields
(defn aggregate [& args]
  (fn [#^Pipe previous]
  (let [[#^Fields in-fields func-fields specs #^Fields out-fields] (parse-args args Fields/ALL)]
    (Every. previous in-fields
      (ClojureAggregator. func-fields specs) out-fields))))

(defn buffer [& args]
  (fn [#^Pipe previous]
    (let [[#^Fields in-fields func-fields specs #^Fields out-fields] (parse-args args Fields/ALL)]
      (Every. previous in-fields
        (ClojureBuffer. func-fields specs) out-fields))))


;; we shouldn't need a seq for fields (b/c we know how many pipes we have)
(defn co-group
  [fields-seq declared-fields joiner]
  (fn [& pipes-seq]
    (CoGroup.
  	  (pipes-array pipes-seq)
  	  (fields-array fields-seq)
  	  (fields declared-fields)
  	  joiner)))

(defn- defop-helper [type spec declared-fields bindings code]
  (let  [[name func-args] (if (sequential? spec)
                          [(clojure.core/first spec) (second spec)]
                          [spec nil])
         runner-name (symbol (str name "__"))
         func-form (if (nil? func-args) `(var ~runner-name) `[(var ~runner-name) ~@func-args])
         args-sym (gensym "args")
         runner-body (if (nil? func-args) `(~bindings ~code) `(~func-args (fn ~bindings ~code)))
         assembly-args (if (nil? func-args) `[ & ~args-sym] `[~func-args & ~args-sym])
        ]
  `(do
    (defn ~runner-name {:fields ~declared-fields} ~@runner-body)
    (defn ~name ~assembly-args
      (apply ~type ~func-form ~args-sym)
      ))))

(defmacro defmapop
  ([spec bindings code] (defmapop spec nil bindings code))
  ([spec declared-fields bindings code]
    (defop-helper 'cascading.clojure.api/map spec declared-fields bindings code)))

(defmacro defmapcatop
  ([spec bindings code] (defmapcatop spec nil bindings code))
  ([spec declared-fields bindings code]
    (defop-helper 'cascading.clojure.api/mapcat spec declared-fields bindings code)))

(defmacro deffilterop
  ([spec bindings code] (deffilterop spec nil bindings code))
  ([spec declared-fields bindings code]
    (defop-helper 'cascading.clojure.api/filter spec declared-fields bindings code)))

(defmacro defaggregateop
  ([spec bindings code] (defaggregateop spec nil bindings code))
  ([spec declared-fields bindings code]
    (defop-helper 'cascading.clojure.api/aggregate spec declared-fields bindings code)))

(defmacro defbufferop
  ([spec bindings code] (defbufferop spec nil bindings code))
  ([spec declared-fields bindings code]
    (defop-helper 'cascading.clojure.api/buffer spec declared-fields bindings code)))

(defn assemble
  ([x] x)
  ([x form] (apply form (collectify x)))
  ([x form & more] (apply assemble (assemble x form) more)))

(defmacro assembly
  ([args return]
    (assembly args [] return))
  ([args bindings return]
    (let [pipify (fn [forms] (if (or (not (sequential? forms))
                                     (vector? forms))
                              forms
                              (cons 'cascading.clojure.api/assemble forms)))
          return (pipify return)
          bindings (vec (clojure.core/map #(%1 %2) (cycle [identity pipify]) bindings))]
      `(fn ~args
          (let ~bindings
            ~return)))))

(defmacro defassembly
  ([name args return]
    (defassembly name args [] return))
  ([name args bindings return]
    `(def ~name (cascading.clojure.api/assembly ~args ~bindings ~return))))

(defn join-assembly [fields-seq declared-fields joiner]
  (assembly [& pipes-seq]
    (pipes-seq (co-group fields-seq declared-fields joiner))))

(defn inner-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (InnerJoin.)))


(defn outer-join [fields-seq declared-fields]
  (join-assembly fields-seq declared-fields (OuterJoin.)))


;;TODO create join abstractions. http://en.wikipedia.org/wiki/Join_(SQL)
;;"join and drop" is called a natural join - inner join, followed by select to remove duplicate join keys.

;;another kind of join and dop is to drop all the join keys - for example, when you have extracted a specil join key jsut for grouping, you typicly want to get rid of it after the group operation.

;;another kind of "join and drop" is an outer-join followed by dropping the nils



(defn taps-map [pipes taps]
  (Cascades/tapsMap (into-array Pipe pipes) (into-array Tap taps)))

(defn mk-flow [sources sinks assembly]
  (let
    [sources (collectify sources)
     sinks (collectify sinks)
     source-pipes (clojure.core/map #(Pipe. (str "spipe" %2)) sources (iterate inc 0))
     tail-pipes (clojure.core/map #(Pipe. (str "tpipe" %2) %1)
                    (collectify (apply assembly source-pipes)) (iterate inc 0))]
     (.connect (FlowConnector.)
        (taps-map source-pipes sources)
        (taps-map tail-pipes sinks)
        (into-array Pipe tail-pipes))))

(defn text-line
 ([]
  (TextLine.))
 ([field-names]
  (TextLine. (fields field-names) (fields field-names))))

(defn sequence-file [field-names]
  (SequenceFile. (fields field-names)))

(defn json-map-line
  [json-keys]
  (let [json-keys-arr (into-array json-keys)
        scheme-fields (fields json-keys)]
    (proxy [Scheme] [scheme-fields scheme-fields]
      (sourceInit [tap #^JobConf conf]
        (.setInputFormat conf TextInputFormat))
      (sinkInit [tap #^JobConf conf]
        (doto conf
          (.setOutputKeyClass Text)
          (.setOutputValueClass Text)
          (.setOutputFormat TextOutputFormat)))
      (source [_ #^Text json-text]
        (let [json-map  (json/parse-string (.toString json-text))
              json-vals (clojure.core/map json-map json-keys-arr)]
          (Util/coerceToTuple json-vals)))
      (sink [#^TupleEntry tuple-entry #^OutputCollector output-collector]
        (let [tuple     (.selectTuple tuple-entry scheme-fields)
              json-map  (areduce json-keys-arr i mem {}
                          (assoc mem (aget json-keys-arr i)
                                     (.get tuple i)))
              json-str  (json/generate-string json-map)]
          (.collect output-collector nil (Tuple. json-str)))))))

(defn path
  {:tag String}
  [x]
  (if (string? x) x (.getAbsolutePath #^File x)))

(defn hfs-tap [#^Scheme scheme path-or-file]
  (Hfs. scheme (path path-or-file)))

(defn lfs-tap [#^Scheme scheme path-or-file]
  (Lfs. scheme (path path-or-file)))


(defn write-dot [#^Flow flow #^String path]
  (.writeDOT flow path))

(defn exec [#^Flow flow]
  (.complete flow))