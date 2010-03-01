(ns cascading.clojure.api-example
  (:import (cascading.tuple Fields))
  (:require (cascading.clojure [api :as c])))


; (c/defassembly c-distinct [pipe]
;   (pipe (c/group-by Fields/ALL) (c/first)))


; (defn starts-with-b? [word]
;   (re-find #"^b.*" word))

; (defn split-words
;   {:fields "word"}
;   [line]
;   (re-seq #"\w+" line))

; (defn uppercase [word]
;   (.toUpperCase word))

(c/deffilterop [starts-with? [s]] [word]
  (re-find (re-pattern (str "^" s ".*")) word))

(c/defmapcatop split-words "word" [line]
  (re-seq #"\w+" line))

(c/defmapop uppercase [word]
  (.toUpperCase word))

(c/defassembly example-assembly [phrase white]
  [phrase (phrase (split-words "line")
                  (starts-with? ["b"])
                  (c/group-by "word")
                  (c/count "count"))
   white (white (split-words "line" :fn> "white"))]
   ([phrase white] (c/inner-join ["word" "white"] ["word" "count" "white"])
                   (c/select ["word" "count"])
                 (uppercase "word" :fn> "upword" :> ["upword" "count"])))


(defn run-example
  [in-phrase-dir-path in-white-dir-path out-dir-path]
  (let [source-scheme  (c/text-line "line")
        sink-scheme    (c/text-line ["upword" "count"])
        phrase-source  (c/hfs-tap source-scheme in-phrase-dir-path)
        white-source   (c/hfs-tap source-scheme in-white-dir-path)
        sink           (c/hfs-tap sink-scheme out-dir-path)
        flow           (c/mk-flow [phrase-source white-source] sink example-assembly)]
    (c/exec flow)))



(comment
  (use 'cascading.clojure.api-example)
  (def root "/Users/mmcgrana/remote/cascading-clojure/")
  (def example-args
    [(str root "cascading-clojure-standalone.jar")
     (str root "data/api-example.dot")
     (str root "data/phrases")
     (str root "data/white")
     (str root "data/output")])
  (apply run-example example-args)
)

(comment
  (use 'cascading.clojure.api-example)
  (def root "/Users/marz/opensource/cascading-clojure/")
  (def example-args
    [(str root "data/phrases")
     (str root "data/white")
     (str root "data/output")])
  (apply run-example example-args)
)
