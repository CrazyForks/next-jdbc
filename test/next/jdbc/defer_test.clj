;; copyright (c) 2024 Sean Corfield, all rights reserved

(ns next.jdbc.defer-test
  "The idea behind the next.jdbc.defer namespace is to provide a
   way to defer the execution of a series of SQL statements until
   a later time, but still provide a way for inserted keys to be
   used in later SQL statements.

   The principle is to provide a core subset of the next.jdbc
   and next.jdbc.sql API that produces a data structure that
   describes a series of SQL operations to be performed, that
   are held in a dynamic var, and that can be executed at a
   later time, in a transaction."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
            [next.jdbc.sql.builder :refer [for-delete for-insert for-update]]
            [next.jdbc.test-fixtures
             :refer [ds with-test-db]]))

(set! *warn-on-reflection* true)

(use-fixtures :once with-test-db)

(def ^:private ^:dynamic *deferred* nil)

(defn execute-one!
  "Given a vector containing a SQL statement and parameters, defer
  execution of that statement."
  ([sql-p]
   (execute-one! sql-p {}))
  ([sql-p opts]
   (swap! *deferred* conj
          {:sql-p   sql-p
           :key-fn  (or (:key-fn opts) (comp first vals))
           :key     (:key opts)
           :opts    opts})))

(defn insert!
  "Given a table name, and a data hash map, defer an insertion of the
  data as a single row in the database."
  ([table key-map]
   (insert! table key-map {}))
  ([table key-map opts]
   (swap! *deferred* conj
          {:sql-p  (for-insert table key-map opts)
           :key-fn (or (:key-fn opts) (comp first vals))
           :key    (:key opts)
           :opts   opts})))

(defn update!
  "Given a table name, a hash map of columns and values to set, and
  either a hash map of columns and values to search on or a vector
  of a SQL where clause and parameters, defer an update on the table."
  ([table key-map where-params]
   (update! table key-map where-params {}))
  ([table key-map where-params opts]
   (swap! *deferred* conj
          {:sql-p  (for-update table key-map where-params opts)
           :opts   opts})))

(defn delete!
  "Given a table name, and either a hash map of columns and values
  to search on or a vector of a SQL where clause and parameters,
  defer a delete on the table."
  ([table where-params]
   (delete! table where-params {}))
  ([table where-params opts]
   (swap! *deferred* conj
          {:sql-p  (for-delete table where-params opts)
           :opts   opts})))

(defn deferrable [transactable stmts]
  (reify clojure.lang.IDeref
    (deref [_]
      (let [keys (atom {})]
        (jdbc/with-transaction [conn transactable]
          (doseq [{:keys [sql-p key-fn key opts]} @stmts]
            (let [sql-p
                  (mapv (fn [v]
                          (if (keyword? v)
                            (if (contains? @keys v)
                              (get @keys v)
                              (throw (ex-info (str "Deferred key not found " v)
                                              {:key v})))
                            v))
                        sql-p)
                  result (jdbc/execute-one! conn sql-p opts)]
              (when key
                (swap! keys assoc key (key-fn result))))))
        @keys))))

(defmacro defer-ops [& body]
  `(binding [*deferred* (atom [])]
     (do ~@body)
     *deferred*))

(defmacro with-deferred [connectable & body]
  `(let [conn# ~connectable]
     (deferrable conn# (defer-ops ~@body))))

(deftest basic-test
  (testing "data structures"
    (is (= [{:sql-p ["INSERT INTO foo (name) VALUES (?)" "Sean"]
             :key-fn :GENERATED_KEY
             :key    :id
             :opts   {:key-fn :GENERATED_KEY :key :id}}]
           @(defer-ops
             (insert! :foo {:name "Sean"} {:key-fn :GENERATED_KEY :key :id})))))
  (testing "execution"
    (let [effects (with-deferred (ds)
                    (insert! :fruit {:name "Mango"} {:key :test}))]
      (is (= {:test 1} @effects))
      (is (= 1 (count (jdbc/execute! (ds)
                                     ["select * from fruit where name = ?"
                                      "Mango"])))))
    (let [effects (with-deferred (ds)
                    (insert! :fruit {:name "Dragonfruit"} {:key :test})
                    (update! :fruit {:cost 123} {:name "Dragonfruit"})
                    (delete! :fruit {:name "Dragonfruit"}))]
      (is (= {:test 1} @effects))
      (is (= 0 (count (jdbc/execute! (ds)
                                     ["select * from fruit where name = ?"
                                      "Dragonfruit"])))))
    (let [effects (with-deferred (ds)
                    (insert! :fruit {:name "Grapefruit" :bad_column 0} {:key :test}))]
      (is (= :failed (try @effects
                          (catch Exception _ :failed))))
      (is (= 0 (count (jdbc/execute! (ds)
                                     ["select * from fruit where name = ?"
                                      "Grapefruit"])))))))
