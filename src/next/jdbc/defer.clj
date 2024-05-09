;; copyright (c) 2024 Sean Corfield, all rights reserved

(ns next.jdbc.defer
  "The idea behind the next.jdbc.defer namespace is to provide a
   way to defer the execution of a series of SQL statements until
   a later time, but still provide a way for inserted keys to be
   used in later SQL statements.

   The principle is to provide a core subset of the next.jdbc
   and next.jdbc.sql API that produces a data structure that
   describes a series of SQL operations to be performed, that
   are held in a dynamic var, and that can be executed at a
   later time, in a transaction."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.sql.builder :refer [for-delete for-insert for-update]]))

(set! *warn-on-reflection* true)

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

(defn defer-ops [f]
  (binding [*deferred* (atom [])]
    (f)
    *deferred*))

(defmacro with-deferred [connectable & body]
  `(let [conn# ~connectable]
     (deferrable conn# (defer-ops (^{:once true} fn* [] ~@body)))))
