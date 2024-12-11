;; copyright (c) 2020-2024 Sean Corfield, all rights reserved

(ns next.jdbc.plan-test
  "Tests for the plan helpers."
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc.plan :as plan]
            [next.jdbc.specs :as specs]
            [next.jdbc.test-fixtures
             :refer [with-test-db ds col-kw index]]
            [clojure.string :as str]))

(set! *warn-on-reflection* true)

;; around each test because of the folding tests using 1,000 rows
(use-fixtures :each with-test-db)

(specs/instrument)

(deftest select-one!-tests
  (is (= {(col-kw :id) 1}
         (plan/select-one! (ds) [(col-kw :id)] [(str "select * from fruit order by " (index))])))
  (is (= 1
         (plan/select-one! (ds) (col-kw :id) [(str "select * from fruit order by " (index))])))
  (is (= "Banana"
         (plan/select-one! (ds) :name [(str "select * from fruit where " (index) " = ?") 2])))
  (is (= [1 "Apple"]
         (plan/select-one! (ds) (juxt (col-kw :id) :name)
                           [(str "select * from fruit order by " (index))])))
  (is (= {(col-kw :id) 1 :name "Apple"}
         (plan/select-one! (ds) #(select-keys % [(col-kw :id) :name])
                           [(str "select * from fruit order by " (index))]))))

(deftest select-vector-tests
  (is (= [{(col-kw :id) 1} {(col-kw :id) 2} {(col-kw :id) 3} {(col-kw :id) 4}]
         (plan/select! (ds) [(col-kw :id)] [(str "select * from fruit order by " (index))])))
  (is (= [1 2 3 4]
         (plan/select! (ds) (col-kw :id) [(str "select * from fruit order by " (index))])))
  (is (= ["Banana"]
         (plan/select! (ds) :name [(str "select * from fruit where " (index) " = ?") 2])))
  (is (= [[2 "Banana"]]
         (plan/select! (ds) (juxt (col-kw :id) :name)
                       [(str "select * from fruit where " (index) " = ?") 2])))
  (is (= [{(col-kw :id) 2 :name "Banana"}]
         (plan/select! (ds) [(col-kw :id) :name]
                       [(str "select * from fruit where " (index) " = ?") 2]))))

(deftest select-set-tests
  (is (= #{{(col-kw :id) 1} {(col-kw :id) 2} {(col-kw :id) 3} {(col-kw :id) 4}}
         (plan/select! (ds) [(col-kw :id)] [(str "select * from fruit order by " (index))]
                       {:into #{}})))
  (is (= #{1 2 3 4}
         (plan/select! (ds) (col-kw :id) [(str "select * from fruit order by " (index))]
                       {:into #{}}))))

(deftest select-map-tests
  (is (= {1 "Apple", 2 "Banana", 3 "Peach", 4 "Orange"}
         (plan/select! (ds) (juxt (col-kw :id) :name) [(str "select * from fruit order by " (index))]
                       {:into {}}))))

(deftest select-issue-227
  (is (= ["Apple"]
         (plan/select! (ds) :name [(str "select * from fruit where " (index) " = ?") 1]
                       {:column-fn #(str/replace % "-" "_")})))
  (is (= ["Apple"]
         (plan/select! (ds) :foo/name [(str "select * from fruit where " (index) " = ?") 1]
                       {:column-fn #(str/replace % "-" "_")})))
  (is (= ["Apple"]
         (plan/select! (ds) #(get % "name") [(str "select * from fruit where " (index) " = ?") 1]
                       {:column-fn #(str/replace % "-" "_")})))
  (is (= [["Apple"]]
         (plan/select! (ds) (juxt :name) [(str "select * from fruit where " (index) " = ?") 1]
                       {:column-fn #(str/replace % "-" "_")}))))
