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
   later time, in a transaction.

   (with-deferred
     (insert! :foo {:name \"Sean\"})
     (insert! :bar {:foo_id (deferred-key :foo) :value 42})
     (insert! :baz {:bar_id (deferred-key :bar) :value 99})
     (update! :foo {:id (deferred-key :foo)} {:name \"Sean Corfield\"})
     (delete! :foo {:id (deferred-key :foo)})
     (delete! :bar {:foo_id (deferred-key :foo)})
     (delete! :baz {:bar_id (deferred-key :bar)}))
   "
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.sql.builder :refer [for-insert]]))

(def ^:private ^:dynamic *deferred* nil)

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

(defn deferable [transactable stmts]
  (reify clojure.lang.IDeref
    (deref [_]
      (let [keys  (atom {})]
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
                  result (jdbc/execute! conn sql-p opts)]
              (when key
                (swap! keys assoc key (key-fn result))))))))))

(defmacro with-deferred [[conn connectable] & body]
  `(let [~conn ~connectable]
     (deferrable ~conn (binding [*deferred* (atom [])] (do ~@body) *deferred*))))

(deftest basic-test
  (is (= [{:sql-p ["INSERT INTO foo (name) VALUES (?)" "Sean"]
           :key-fn :GENERATED_KEY
           :key    :id
           :opts   {:key-fn :GENERATED_KEY :key :id}}]
         (binding [*deferred* (atom [])]
           (insert! :foo {:name "Sean"} {:key-fn :GENERATED_KEY :key :id})
           @*deferred*))))
