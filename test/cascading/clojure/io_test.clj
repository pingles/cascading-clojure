(ns cascading.clojure.io-test
  (:use clojure.test)
  (:use cascading.clojure.io))

(deftest encode-json-test
  (is (= "{\"foo\":1,\"bar\":2}"
         (encode-json {"foo" 1 "bar" 2})))
  (is (= "{\"foo\":1,\"bar\":2}"
         (encode-json {:foo 1 :bar 2})))
  (is (= "{\"foo\":{\"bar\":1}}"
         (encode-json {:foo {:bar 1}}))))

(deftest decode-json-test
  (is (= {:foo 1 :bar 2}
         (decode-json "{\"foo\":1,\"bar\":2}")))
  (is (= {:foo {:bar 1}}
         (decode-json "{\"foo\":{\"bar\":1}}")))
  (is (= ["foo" {:bar 1}]
         (decode-json "[\"foo\",{\"bar\":1}]"))))
