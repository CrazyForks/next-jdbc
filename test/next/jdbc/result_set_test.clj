;; copyright (c) 2019-2024 Sean Corfield, all rights reserved

(ns next.jdbc.result-set-test
  "Test namespace for the result set functions.

  What's left to be tested:
  * ReadableColumn protocol extension point"
  (:require [clojure.core.protocols :as core-p]
            [clojure.datafy :as d]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc.protocols :as p]
            [next.jdbc.result-set :as rs]
            [next.jdbc.specs :as specs]
            [next.jdbc.test-fixtures :refer [with-test-db ds column index col-kw
                                             default-options
                                             derby? mssql? mysql? postgres? xtdb?]])
  (:import (java.sql ResultSet ResultSetMetaData)))

(set! *warn-on-reflection* true)

(use-fixtures :once with-test-db)

(specs/instrument)

(deftest test-datafy-nav
  (testing "default schema"
    (let [connectable (ds)
          test-row (rs/datafiable-row {:TABLE/FRUIT_ID 1} connectable
                                      (cond-> (default-options)
                                        (xtdb?)
                                        (assoc :schema-opts {:pk "_id"})))
          data (d/datafy test-row)
          v (get data :TABLE/FRUIT_ID)]
      ;; check datafication is sane
      (is (= 1 v))
      (let [object (d/nav data :table/fruit_id v)]
        ;; check nav produces a single map with the expected key/value data
        (is (= 1 ((column :FRUIT/ID) object)))
        (is (= "Apple" ((column :FRUIT/NAME) object))))))
  (testing "custom schema *-to-1"
    (let [connectable (ds)
          test-row (rs/datafiable-row {:foo/bar 2} connectable
                                      (assoc (default-options)
                                             :schema {:foo/bar
                                                      (if (xtdb?)
                                                        :fruit/_id
                                                        :fruit/id)}))
          data (d/datafy test-row)
          v (get data :foo/bar)]
      ;; check datafication is sane
      (is (= 2 v))
      (let [object (d/nav data :foo/bar v)]
        ;; check nav produces a single map with the expected key/value data
        (is (= 2 ((column :FRUIT/ID) object)))
        (is (= "Banana" ((column :FRUIT/NAME) object))))))
  (testing "custom schema *-to-many"
    (let [connectable (ds)
          test-row (rs/datafiable-row {:foo/bar 3} connectable
                                      (assoc (default-options)
                                             :schema {:foo/bar
                                                      [(if (xtdb?)
                                                         :fruit/_id
                                                         :fruit/id)]}))
          data (d/datafy test-row)
          v (get data :foo/bar)]
      ;; check datafication is sane
      (is (= 3 v))
      (let [object (d/nav data :foo/bar v)]
        ;; check nav produces a result set with the expected key/value data
        (is (vector? object))
        (is (= 3 ((column :FRUIT/ID) (first object))))
        (is (= "Peach" ((column :FRUIT/NAME) (first object)))))))
  (testing "legacy schema tuples"
    (let [connectable (ds)
          test-row (rs/datafiable-row {:foo/bar 2} connectable
                                      (assoc (default-options)
                                             :schema {:foo/bar [:fruit (col-kw :id)]}))
          data (d/datafy test-row)
          v (get data :foo/bar)]
      ;; check datafication is sane
      (is (= 2 v))
      (let [object (d/nav data :foo/bar v)]
        ;; check nav produces a single map with the expected key/value data
        (is (= 2 ((column :FRUIT/ID) object)))
        (is (= "Banana" ((column :FRUIT/NAME) object)))))
    (let [connectable (ds)
          test-row (rs/datafiable-row {:foo/bar 3} connectable
                                      (assoc (default-options)
                                             :schema {:foo/bar [:fruit (col-kw :id) :many]}))
          data (d/datafy test-row)
          v (get data :foo/bar)]
      ;; check datafication is sane
      (is (= 3 v))
      (let [object (d/nav data :foo/bar v)]
        ;; check nav produces a result set with the expected key/value data
        (is (vector? object))
        (is (= 3 ((column :FRUIT/ID) (first object))))
        (is (= "Peach" ((column :FRUIT/NAME) (first object))))))))

(deftest test-map-row-builder
  (testing "default row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select * from fruit where " (index) " = ?") 1]
                              (default-options))]
      (is (map? row))
      (is (contains? row (column :FRUIT/GRADE)))
      (is (nil? ((column :FRUIT/GRADE) row)))
      (is (= 1 ((column :FRUIT/ID) row)))
      (is (= "Apple" ((column :FRUIT/NAME) row))))
    (let [rs (p/-execute-all (ds)
                             [(str "select * from fruit order by " (index))]
                             (default-options))]
      (is (every? map? rs))
      (is (= 1 ((column :FRUIT/ID) (first rs))))
      (is (= "Apple" ((column :FRUIT/NAME) (first rs))))
      (is (= 4 ((column :FRUIT/ID) (last rs))))
      (is (= "Orange" ((column :FRUIT/NAME) (last rs))))))
  (testing "unqualified row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select * from fruit where " (index) " = ?") 2]
                              {:builder-fn rs/as-unqualified-maps})]
      (is (map? row))
      (is (contains? row (column :COST)))
      (is (nil? ((column :COST) row)))
      (is (= 2 ((column :ID) row)))
      (is (= "Banana" ((column :NAME) row)))))
  (testing "lower-case row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select * from fruit where " (index) " = ?") 3]
                              (assoc (default-options)
                                     :builder-fn rs/as-lower-maps))]
      (is (map? row))
      (is (contains? row (col-kw :fruit/appearance)))
      (is (nil? ((col-kw :fruit/appearance) row)))
      (is (= 3 ((col-kw :fruit/id) row)))
      (is (= "Peach" ((col-kw :fruit/name) row)))))
  (testing "unqualified lower-case row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select * from fruit where " (index) " = ?") 4]
                              {:builder-fn rs/as-unqualified-lower-maps})]
      (is (map? row))
      (is (= 4 ((col-kw :id) row)))
      (is (= "Orange" ((col-kw :name) row)))))
  (testing "kebab-case row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select " (index) ",name,appearance as looks_like from fruit where " (index) " = ?") 3]
                              (assoc (default-options)
                                     :builder-fn rs/as-kebab-maps))]
      (is (map? row))
      (is (contains? row (col-kw :fruit/looks-like)))
      (is (nil? ((col-kw :fruit/looks-like) row)))
      ;; kebab-case strips leading _ from _id (XTDB):
      (is (= 3 ((if (xtdb?) :id :fruit/id) row)))
      (is (= "Peach" ((col-kw :fruit/name) row)))))
  (testing "unqualified kebab-case row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select " (index) ",name,appearance as looks_like from fruit where " (index) " = ?") 4]
                              {:builder-fn rs/as-unqualified-kebab-maps})]
      (is (map? row))
      (is (contains? row :looks-like))
      (is (= "juicy" (:looks-like row)))
      (is (= 4 (:id row)))
      (is (= "Orange" (:name row)))))
  (testing "custom row builder 1"
    (let [row (p/-execute-one (ds)
                              [(str "select fruit.*, " (index) " + 100 as newid from fruit where " (index) " = ?") 3]
                              (assoc (default-options)
                                     :builder-fn rs/as-modified-maps
                                     :label-fn str/lower-case
                                     :qualifier-fn identity))]
      (is (map? row))
      (is (contains? row (column :FRUIT/appearance)))
      (is (nil? ((column :FRUIT/appearance) row)))
      (is (= 3 ((column :FRUIT/id) row)))
      (is (= 103 (:newid row))) ; no table name here
      (is (= "Peach" ((column :FRUIT/name) row)))))
  (testing "custom row builder 2"
    (let [row (p/-execute-one (ds)
                              [(str "select fruit.*, " (index) " + 100 as newid from fruit where " (index) " = ?") 3]
                              (assoc (default-options)
                                     :builder-fn rs/as-modified-maps
                                     :label-fn str/lower-case
                                     :qualifier-fn (constantly "vegetable")))]
      (is (map? row))
      (is (contains? row :vegetable/appearance))
      (is (nil? (:vegetable/appearance row)))
      (is (= 3 ((if (xtdb?) :vegetable/_id :vegetable/id) row)))
      (is (= 103 (:vegetable/newid row))) ; constant qualifier here
      (is (= "Peach" (:vegetable/name row)))))
  (testing "adapted row builder"
    (let [row (p/-execute-one (ds)
                              [(str "select * from fruit where " (index) " = ?") 3]
                              (assoc
                               (default-options)
                               :builder-fn (rs/as-maps-adapter
                                            rs/as-modified-maps
                                            (fn [^ResultSet rs
                                                 ^ResultSetMetaData rsmeta
                                                 ^Integer i]
                                              (condp = (.getColumnType rsmeta i)
                                                     java.sql.Types/VARCHAR
                                                     (.getString rs i)
                                                     java.sql.Types/INTEGER
                                                     (.getLong rs i)
                                                     (.getObject rs i))))
                               :label-fn str/lower-case
                               :qualifier-fn identity))]
      (is (map? row))
      (is (contains? row (column :FRUIT/appearance)))
      (is (nil? ((column :FRUIT/appearance) row)))
      (is (= 3 ((column :FRUIT/id) row)))
      (is (= "Peach" ((column :FRUIT/name) row))))
    (let [builder (rs/as-maps-adapter
                   rs/as-modified-maps
                   (fn [^ResultSet rs _ ^Integer i]
                     (.getObject rs i)))
          row (p/-execute-one (ds)
                              [(str "select * from fruit where " (index) " = ?") 3]
                              (assoc
                               (default-options)
                               :builder-fn (rs/as-maps-adapter
                                            builder
                                            (fn [^ResultSet rs
                                                 ^ResultSetMetaData rsmeta
                                                 ^Integer i]
                                              (condp = (.getColumnType rsmeta i)
                                                     java.sql.Types/VARCHAR
                                                     (.getString rs i)
                                                     java.sql.Types/INTEGER
                                                     (.getLong rs i)
                                                     (.getObject rs i))))
                               :label-fn str/lower-case
                               :qualifier-fn identity))]
      (is (map? row))
      (is (contains? row (column :FRUIT/appearance)))
      (is (nil? ((column :FRUIT/appearance) row)))
      (is (= 3 ((column :FRUIT/id) row)))
      (is (= "Peach" ((column :FRUIT/name) row))))))

(deftest test-row-number
  ;; two notes here: we use as-arrays as a nod to issue #110 to make
  ;; sure that actually works; also Apache Derby is the only database
  ;; (that we test against) to restrict .getRow() calls to scroll cursors
  (testing "row-numbers on bare abstraction"
    (is (= [1 2 3]
           (into [] (map rs/row-number)
                 (p/-execute (ds) [(str "select * from fruit where " (index) " < ?") 4]
                             ;; we do not need a real builder here...
                             (cond-> {:builder-fn (constantly nil)}
                                     (derby?)
                                     (assoc :concurrency :read-only
                                            :cursors     :close
                                            :result-type :scroll-insensitive)))))))
  (testing "row-numbers on realized row"
    (is (= [1 2 3]
           (into [] (comp (map #(rs/datafiable-row % (ds) {}))
                          (map rs/row-number))
                 (p/-execute (ds) [(str "select * from fruit where " (index) " < ?") 4]
                             ;; ...but datafiable-row requires a real builder
                             (cond-> {:builder-fn rs/as-arrays}
                                     (derby?)
                                     (assoc :concurrency :read-only
                                            :cursors     :close
                                            :result-type :scroll-insensitive))))))))

(deftest test-column-names
  (testing "column-names on bare abstraction"
    (is (= #{(index) "appearance" "grade" "cost" "name"}
           (reduce (fn [_ row]
                     (-> row
                         (->> (rs/column-names)
                              (map (comp str/lower-case name))
                              (set)
                              (reduced))))
                   nil
                   (p/-execute (ds) [(str "select * from fruit where " (index) " < ?") 4]
                               ;; column-names require a real builder
                               {:builder-fn rs/as-arrays})))))
  (testing "column-names on realized row"
    (is (= #{(index) "appearance" "grade" "cost" "name"}
           (reduce (fn [_ row]
                     (-> row
                         (rs/datafiable-row (ds) {})
                         (->> (rs/column-names)
                              (map (comp str/lower-case name))
                              (set)
                              (reduced))))
                   nil
                   (p/-execute (ds) [(str "select * from fruit where " (index) " < ?") 4]
                               {:builder-fn rs/as-arrays}))))))

(deftest test-over-partition-all
  ;; this verifies that InspectableMapifiedResultSet survives partition-all
  (testing "row-numbers on partitioned rows"
    (is (= [[1 2] [3 4]]
           (into [] (comp (map #(rs/datafiable-row % (ds) %))
                          (partition-all 2)
                          (map #(map rs/row-number %)))
                 (p/-execute (ds) ["select * from fruit"]
                             (cond-> {:builder-fn rs/as-arrays}
                                     (derby?)
                                     (assoc :concurrency :read-only
                                            :cursors     :close
                                            :result-type :scroll-insensitive))))))))

(deftest test-mapify
  (testing "no row builder is used"
    (is (= [true]
           (into [] (map map?) ; it looks like a real map now
                 (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 1]
                             {:builder-fn (constantly nil)}))))
    (is (= ["Apple"]
           (into [] (map :name) ; keyword selection works
                 (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 1]
                             {:builder-fn (constantly nil)}))))
    (is (= [[2 [:name "Banana"]]]
           (into [] (map (juxt #(get % (index)) ; get by string key works
                               #(find % :name))) ; get MapEntry works
                 (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 2]
                             {:builder-fn (constantly nil)}))))
    (is (= [{(col-kw :id) 3 :name "Peach"}]
           (into [] (map #(select-keys % [(col-kw :id) :name])) ; select-keys works
                 (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 3]
                             {:builder-fn (constantly nil)}))))
    (is (= [[:orange 4]]
           (into [] (map #(vector (if (contains? % :name) ; contains works
                                    (keyword (str/lower-case (:name %)))
                                    :unnamed)
                                  (get % (col-kw :id) 0))) ; get with not-found works
                 (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 4]
                             {:builder-fn (constantly nil)}))))
    (is (= [{}]
           (into [] (map empty) ; return empty map without building
                 (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 1]
                             {:builder-fn (constantly nil)})))))
  (testing "count does not build a map"
    (let [count-builder (fn [_1 _2]
                          (reify rs/RowBuilder
                            (column-count [_] 13)))]
      (is (= [13]
             (into [] (map count) ; count relies on columns, not row fields
                   (p/-execute (ds) [(str "select * from fruit where " (index) " = ?") 1]
                               {:builder-fn count-builder}))))))
  (testing "assoc, dissoc, cons, seq, and = build maps"
    (is (map? (reduce (fn [_ row] (reduced (assoc row :x 1)))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (= 6 (count (reduce (fn [_ row] (reduced (assoc row :x 1)))
                            nil
                            (p/-execute (ds) ["select * from fruit"] {})))))
    (is (map? (reduce (fn [_ row] (reduced
                                   (dissoc row (column :FRUIT/NAME))))
                      nil
                      (p/-execute (ds) ["select * from fruit"]
                                  (default-options)))))
    (is (= 4 (count (reduce (fn [_ row] (reduced
                                         (dissoc row (column :FRUIT/NAME))))
                            nil
                            (p/-execute (ds) ["select * from fruit"]
                                        (default-options))))))
    (is (seq? (reduce (fn [_ row] (reduced (seq row)))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (seq? (reduce (fn [_ row] (reduced (cons :seq row)))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (= :seq (first (reduce (fn [_ row] (reduced (cons :seq row)))
                               nil
                               (p/-execute (ds) ["select * from fruit"] {})))))
    (is (false? (reduce (fn [_ row] (reduced (= row {})))
                        nil
                        (p/-execute (ds) ["select * from fruit"] {}))))
    (is (map-entry? (second (reduce (fn [_ row] (reduced (cons :seq row)))
                                    nil
                                    (p/-execute (ds) ["select * from fruit"] {})))))
    (is (every? map-entry? (reduce (fn [_ row] (reduced (seq row)))
                                   nil
                                   (p/-execute (ds) ["select * from fruit"] {}))))
    (is (map? (reduce (fn [_ row] (reduced (conj row {:a 1})))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (map? (reduce (fn [_ row] (reduced (conj row [:a 1])))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (map? (reduce (fn [_ row] (reduced (conj row {:a 1 :b 2})))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (= 1 (:a (reduce (fn [_ row] (reduced (conj row {:a 1})))
                         nil
                         (p/-execute (ds) ["select * from fruit"] {})))))
    (is (= 1 (:a (reduce (fn [_ row] (reduced (conj row [:a 1])))
                         nil
                         (p/-execute (ds) ["select * from fruit"] {})))))
    (is (= 1 (:a (reduce (fn [_ row] (reduced (conj row {:a 1 :b 2})))
                         nil
                         (p/-execute (ds) ["select * from fruit"] {})))))
    (is (= 2 (:b (reduce (fn [_ row] (reduced (conj row {:a 1 :b 2})))
                         nil
                         (p/-execute (ds) ["select * from fruit"] {})))))
    (is (vector? (reduce (fn [_ row] (reduced (conj row :a)))
                         nil
                         (p/-execute (ds) ["select * from fruit"]
                                     {:builder-fn rs/as-arrays}))))
    (is (= :a (peek (reduce (fn [_ row] (reduced (conj row :a)))
                            nil
                            (p/-execute (ds) ["select * from fruit"]
                                        {:builder-fn rs/as-arrays})))))
    (is (= :b (peek (reduce (fn [_ row] (reduced (conj row :a :b)))
                            nil
                            (p/-execute (ds) ["select * from fruit"]
                                        {:builder-fn rs/as-arrays}))))))
  (testing "datafiable-row builds map; with metadata"
    (is (map? (reduce (fn [_ row] (reduced (rs/datafiable-row row (ds) {})))
                      nil
                      (p/-execute (ds) ["select * from fruit"] {}))))
    (is (contains? (meta (reduce (fn [_ row] (reduced (rs/datafiable-row row (ds) {})))
                                 nil
                                 (p/-execute (ds) ["select * from fruit"] {})))
                   `core-p/datafy))))

;; test that we can create a record-based result set builder:

(defrecord Fruit [id name appearance cost grade])

(defn fruit-builder [^ResultSet rs ^ResultSetMetaData rsmeta]
  (reify
    rs/RowBuilder
    (->row [_] (->Fruit (.getObject rs ^String (index))
                        (.getObject rs "name")
                        (.getObject rs "appearance")
                        (.getObject rs "cost")
                        (.getObject rs "grade")))
    (column-count [_] 0) ; no need to iterate over columns
    (with-column [_ row i] row)
    (with-column-value [_ row col v] row)
    (row! [_ row] row)
    rs/ResultSetBuilder
    (->rs [_] (transient []))
    (with-row [_ rs row] (conj! rs row))
    (rs! [_ rs] (persistent! rs))
    clojure.lang.ILookup ; only supports :cols and :rsmeta
    (valAt [this k] (get this k nil))
    (valAt [this k not-found]
      (case k
        :cols [(col-kw :id) :name :appearance :cost :grade]
        :rsmeta rsmeta
        not-found))))

(deftest custom-map-builder
  (let [row (p/-execute-one (ds)
                            ["select * from fruit where appearance = ?" "red"]
                            {:builder-fn fruit-builder})]
    (is (instance? Fruit row))
    (is (= 1 (:id row))))
  (let [rs (p/-execute-all (ds)
                           ["select * from fruit where appearance = ?" "red"]
                           {:builder-fn fruit-builder})]
    (is (every? #(instance? Fruit %) rs))
    (is (= 1 (count rs)))
    (is (= 1 (:id (first rs))))))

(deftest metadata-result-set
  (let [metadata (with-open [con (p/get-connection (ds) {})]
                   (-> (.getMetaData con)
                       (.getTables nil nil nil (into-array ["TABLE" "VIEW"]))
                       (rs/datafiable-result-set (ds) {})))]
    (is (vector? metadata))
    (is (map? (first metadata)))
    ;; we should find :something/table_name with a value of "fruit"
    ;; may be upper/lower-case, could have any qualifier
    (is (some (fn [row]
                (some #(and (= "table_name" (-> % key name str/lower-case))
                            (= "fruit" (-> % val name str/lower-case)))
                      row))
              metadata))))

(deftest clob-reading
  (when-not (or (mssql?) (mysql?) (postgres?) (xtdb?)) ; no clob in these
    (with-open [con (p/get-connection (ds) {})]
      (try
        (p/-execute-one con ["DROP TABLE CLOBBER"] {})
        (catch Exception _))
      (p/-execute-one con [(str "
CREATE TABLE CLOBBER (
  ID INTEGER,
  STUFF CLOB
)")]
                      {})
      (p/-execute-one con
                      [(str "insert into clobber (id, stuff)"
                            "values (?,?), (?,?)")
                       1 "This is some long string"
                       2 "This is another long string"]
                      {})
      (is (= "This is some long string"
             (-> (p/-execute-all con
                                 ["select * from clobber where id = ?" 1]
                                 {:builder-fn (rs/as-maps-adapter
                                               rs/as-unqualified-lower-maps
                                               rs/clob-column-reader)})
                 (first)
                 :stuff))))))

(deftest test-get-n-array
  (testing "get n on bare abstraction over arrays"
    (is (= [1 2 3]
           (into [] (map #(get % 0))
                 (p/-execute (ds) [(str "select " (index) " from fruit where " (index) " < ? order by " (index)) 4]
                             {:builder-fn rs/as-arrays})))))
  (testing "nth on bare abstraction over arrays"
    (is (= [1 2 3]
           (into [] (map #(nth % 0))
                 (p/-execute (ds) [(str "select " (index) " from fruit where " (index) " < ? order by " (index)) 4]
                             {:builder-fn rs/as-arrays}))))))
