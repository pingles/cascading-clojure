(ns cascading.clojure.io
  (:import java.io.File
           java.util.UUID
           clojure.lang.Keyword
           (org.apache.log4j Logger Level))
  (:require [clojure.contrib.java-utils :as ju]
            [clojure.contrib.duck-streams :as ds]
            [clj-json.core :as json]))

(def encode-json
  json/generate-string)

(defn keyify [obj]
  (cond
    (map? obj)
      (reduce (fn [i [#^String k v]]
                (assoc i (Keyword/intern k) (keyify v))) {} obj)
    (vector? obj)
      (vec (map keyify obj))
    :else
      obj))

(defn decode-json [obj]
  (keyify (json/parse-string obj)))

(defn temp-path [sub-path]
   (ju/file (System/getProperty "java.io.tmpdir") sub-path))

(defn temp-dir
  "1) creates a directory in System.getProperty(\"java.io.tmpdir\")
   2) calls tempDir.deleteOn Exit() so the file is deleted by the jvm.
   reference: ;http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4735419
   deleteOnExit is last resort cleanup on jvm exit."
  [sub-path]
  (let [tmp-dir (temp-path sub-path)]
    (or (.exists tmp-dir) (.mkdir tmp-dir))
    (.deleteOnExit tmp-dir)
    tmp-dir))

(defn delete-all
  "delete-file-recursively is preemptive delete on exiting the code block for
   repl and tests run in the same process."
  [bindings]
  (doseq [file (reverse (map second (partition 2 bindings)))]
    (if (.exists file)
     (ju/delete-file-recursively file))))

(defmacro with-tmp-files [bindings & body]
  `(let ~bindings
     (try ~@body
       (finally (delete-all ~bindings)))))

(defn- uuid []
  (str (UUID/randomUUID)))

(defn write-lines-in
  ([root lines]
   (write-lines-in root (str (uuid) ".data") lines))
  ([root filename lines]
   (ds/write-lines
     (ju/file (.getAbsolutePath root) filename) lines)))

(def log-levels
  {:fatal Level/FATAL
   :warn  Level/WARN
   :info  Level/INFO
   :debug Level/DEBUG
   :off   Level/OFF})

(defmacro with-log-level [level & body]
  `(let [with-lev#  (log-levels ~level)
         logger#    (Logger/getRootLogger)
         prev-lev#  (.getLevel logger#)]
     (try
       (.setLevel logger# with-lev#)
       ~@body
       (finally
         (.setLevel logger# prev-lev#)))))
