(ns computesoftware.postgres-test
  (:require
    [clojure.test :refer :all]
    [computesoftware.postgres :as postgres]))

(deftest ensure-ident-fn-test
  (let [ifn (postgres/ensure-ident-fn {:max-identifier-length 5})]
    (is (= "a" (ifn "a")))
    (is (= "abcd" (ifn "abcd")))
    (is (= "abc:0" (ifn "abcdf"))
      ">= 5 chars gets truncated")
    (is (= "abc:1" (ifn "abcdfg"))
      "counter increments")
    (is (= "abc:0" (ifn "abcdf"))
      "repeated call gets same suffix")
    (is (= "a" (ifn "a")))
    (is (= "ét:2" (ifn "étonné"))
      "limit is in bytes, multibytes characters count more.")))
