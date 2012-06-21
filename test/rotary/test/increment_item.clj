(ns rotary.test.increment-item
  (:use rotary.client
        clojure.test
        clojure.stacktrace))

(def table "inc-item-test-tbl")
(def hash-key {:name "hash-key" :type "S"})
(def attr "value") ; can also be thought of as column
(def cred {:access-key "myAccessKey" :secret-key "mySecretKey"})

(defn setup-table []
  (create-table cred {:name table :hash-key hash-key :throughput {:read 10
                                                                   :write 5}})
  (while (not= :active (:status (describe-table cred table)))
    (println "Waiting to create table" table)
    (Thread/sleep 10000)))

(defn remove-table []
  (delete-table cred table)
  (while (some #{table} (list-tables cred))
    (println "Waiting to delete table" table)
    (Thread/sleep 10000)))

(defmacro wrap-test [& body]
  `(do
     (try
       (setup-table)
       ~@body
       (catch Throwable t# (print-cause-trace t#))
       (finally (remove-table)))))

(deftest inc-empty
  (wrap-test
    (is (= 1 (increment-item cred table "akey" attr 1))
        "Incrementing an empty value should start from 0.")))

(deftest inc-existing
  (wrap-test
    (put-item cred table {(:name hash-key) "mykey"
                          attr 3})
    (is (= 10 (increment-item cred table "mykey" attr 7))
        "Incrementing by 7 should give back 10.")))

