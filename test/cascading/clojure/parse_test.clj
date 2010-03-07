(ns cascading.clojure.parse-test
  (:use clojure.test
        (cascading.clojure parse testing))
  (:import cascading.tuple.Fields))

(defn example [x] x)

(def obj-array-class (class (into-array Object [])))

(defn extract-obj-array [parse-results]
  (map (fn [e]
         (if (instance? obj-array-class e) (seq e) e))
       parse-results))

(deftest parse-everything
  (is (= [(fields ["foo"])
	        (fields ["bar"])
	        ["cascading.clojure.parse-test" "example"]
	        (fields ["baz"])]
	  (extract-obj-array
	    (parse-args [#'example "foo" :fn> "bar" :> "baz"])))))

(deftest parse-everything-multiple-ins
  (is (= [(fields ["foo" "bat"])
	        (fields ["bar"])
	        ["cascading.clojure.parse-test" "example"]
	        (fields ["baz"])]
	  (extract-obj-array
	   (parse-args [#'example ["foo" "bat"] :fn> "bar" :> "baz"])))))

(deftest parse-no-input-selectors
  (is (= [Fields/ALL
	        (fields ["bar"])
	        ["cascading.clojure.parse-test" "example"]
	        (fields ["baz"])]
	  (extract-obj-array
	    (parse-args [#'example :fn> "bar" :> "baz"])))))

(deftest parse-no-input-or-output-selectors
  (is (= [Fields/ALL
	        (fields ["bar"])
	        ["cascading.clojure.parse-test" "example"]
	        Fields/RESULTS]
	  (extract-obj-array
	    (parse-args [#'example :fn> "bar"])))))

(deftest parse-no-input-or-output-or-fn-selectors
  (is (= [Fields/ALL
	        Fields/ARGS
	        ["cascading.clojure.parse-test" "example"]
	        Fields/RESULTS]
	  (extract-obj-array
	    (parse-args [#'example])))))
