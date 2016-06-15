(ns financial-senti.mongo
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [financial-senti.time :as t]
            [incanter.stats :as s]
            [clojure-csv.core :as clj-csv])
  (:import [com.mongodb MongoOptions ServerAddress]
           org.bson.types.ObjectId))

#_(defn directory
    []
    (io/file "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\export"))

(defn directory
  []
  (io/file "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\export2"))


(defn files
  []
  (file-seq (directory)))

(defn lazy-read-csv
  [csv-file]
  (let [in-file (io/reader csv-file)
        csv-seq (csv/read-csv in-file)
        lazy (fn lazy [wrapped]
               (lazy-seq
                 (if-let [s (seq wrapped)]
                   (cons (first s) (lazy (rest s)))
                   (.close in-file))))]
    (lazy csv-seq)))

(defn board-to-long
  [data]
  (let [horizontal (doall (rest (first data)))]
    (->>
      (map
        (fn [row]
          (map
            conj
            (map
              #(vector (first row) %)
              (rest row))
            horizontal))
        (rest data))
      (mapcat identity)
      (remove #(or (= (second %) "") (nil? (second %)))))))

(defn lazy-read-csv-head-on
  [file]
  (let [coll (lazy-read-csv file)
        head (map keyword (first coll))
        rows (rest coll)]
    (map #(zipmap head %) rows)))

(defn write-to-mongo-in-batch
  [db coll lazy-data]
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (mc/insert-batch db coll lazy-data)))

(defn write-first-to-mongo
  [db coll]
  (let [conn (mg/connect)
        db (mg/get-db conn db)
        f (first (rest (files)))]
    (doseq [d (lazy-read-csv-head-on f)]
      (mc/insert db coll (assoc d :_id (ObjectId.))))))


(defn unique-index
  [db coll key]
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (mc/ensure-index db coll (array-map key 1) {:unique true})))

(defn ensure-index
  [db coll & keyvals]
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (mc/ensure-index db coll (apply array-map keyvals))))


#_(defn update-to-mongo
    [db coll identi]
    (let [conn (mg/connect)
          db (mg/get-db conn db)
          a (atom 0)]
      (doseq [f (doall (rest (rest (files))))]
        (doseq [d (lazy-read-csv-head-on f)]
          (mc/update db coll {identi (get d identi)}
                     {$set d} {:upsert true})
          (swap! a inc)
          (if (= 0 (mod @a 5000))
            (print d))))))

(defn update-to-mongo
  [db coll identi]
  (let [conn (mg/connect)
        db (mg/get-db conn db)
        a (atom 0)]
    (doseq [f (doall (rest (files)))]
      (doseq [d (lazy-read-csv-head-on f)]
        (mc/update db coll {identi (get d identi)}
                   {$set d} {:upsert true})
        (swap! a inc)
        (if (= 0 (mod @a 5000))
          (print d))))))

(defn convert-date
  [db coll old-date new-date index]
  (let [conn (mg/connect)
        db (mg/get-db conn db)
        a (atom 0)]
    (dorun
      (map
        #(do
          (mc/update db coll {index (get % index)}
                     {$set {new-date (t/parse-date (get % old-date))}}
                     {:upsert true})
          (swap! a inc)
          (if (= 0 (mod @a 5000))
            (print %)))
        (mc/find-maps db coll {} [index old-date])))))

(defn read-into-memory
  "Used to read unique values of 2 fields into memory"
  [db coll & ks]
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (apply
      (partial conj {})
      (doall
        (map
          (apply juxt ks)
          (mc/find-maps db coll {} ks))))))

(defn find-one
  [db coll cond ks]
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (mc/find-one-as-map db coll cond ks)))

(defn convert-variable
  [db coll old-v new-v index func]
  (let [conn (mg/connect)
        db (mg/get-db conn db)
        a (atom 0)]
    (dorun
      (pmap
        #(do
          (mc/update db coll {index (get % index)}
                     {$set {new-v (func (get % old-v))}}
                     {:upsert true})
          (swap! a inc)
          (if (= 0 (mod @a 5000))
            (print %)))
        (mc/find-maps db coll {} [index old-v])))))

(defn convert-variables
  [db coll old-vs new-v index func]
  (let [conn (mg/connect)
        db (mg/get-db conn db)
        a (atom 0)]
    (dorun
      (pmap
        #(do
          (mc/update db coll {index (get % index)}
                     {$set {new-v (func ((apply juxt old-vs)
                                          %))}}
                     {:upsert true})
          (swap! a inc)
          (if (= 0 (mod @a 5000))
            (print %)))
        (mc/find-maps db coll {} (conj old-vs index))))))

(def index (read-into-memory "finance" "index"
                             :issuer_cusip
                             :permno))

(defn match-permno
  [isin]
  (when (> (count isin) 8)
    (let [issuer_cusip (subs isin 2 8)]
      (get index issuer_cusip))))

(def index2 (read-into-memory "finance" "index"
                             :issuer_cusip
                             :permno))

(defn match-permno-general
  [index key [head end]]
  (when (> (count key) 8)
    (let [issuer_cusip (subs key head end)]
      (get index issuer_cusip))))

(def match-cusip-permno
  (fn [cusip]
    (match-permno-general index2 cusip [0 6])))

(defn match-isin-permno
  [isin]
  (match-permno-general index2 isin [2 8]))



(defn query-date-span
  [db coll date-v match-v]                         ;target collection
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (fn [target-v start end stat-func]             ;time span
      (fn [[date match]]                           ;function as input of convert-variables
        (when (and match (or (not (string? match)) (> (count match) 0)))
          (let [span (t/date-span start end date)]
            (->>
              (mc/find-maps
                db coll
                {match-v match
                 date-v  {$gte (first span)
                          $lte (second span)}}
                [target-v])
              (map target-v)
              stat-func)))))))

(defn query-cond-date-span
  [db coll date-v match-v]                                  ;define target collection
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (fn [cond-vs filter-func]                               ;define conditions by other variables
      (fn [target-v start end stat-func]                    ;define time span
        (fn [[date match]]                                  ;return function as input of function convert-variables
          (when (and match (or (not (string? match)) (> (count match) 0)))
            (let [span (t/date-span start end date)]
              (->>
                (mc/find-maps
                  db coll
                  {match-v match
                   date-v  {$gte (first span)
                            $lte (second span)}}
                  (conj cond-vs target-v))
                (filter-func cond-vs target-v)
                stat-func))))))))


(defn ignore-nil
  [func]
  (fn [input]
    (when (and input (> (count input) 0))
      (func input))))

(defn filter-nil
  [func]
  (fn [input]
    (let [l (remove nil? input)]
      (when (first l)
        (func l)))))


(defn to-catch
  [func]
  (fn [input]
    (->> input
         (map (ignore-nil read-string))
         (#((filter-nil func) %))
         )))

(def median (to-catch s/median))
(def mean (to-catch s/mean))


(defn filter-by-other-conds
  [pred-func]
  (fn [[unique-v filter-v] target-v data]
    (let [data (filter
                 #(pred-func (get % filter-v))
                 data)
          acm {target-v [] unique-v #{}}]
      (get
        (reduce
          (fn [acm el]
            (let [a (target-v acm)
                  b (unique-v acm)
                  c (target-v el)
                  d (unique-v el)]
              (if (get b d)
                {target-v a unique-v b}
                {target-v (conj a c) unique-v (conj b d)})))
          acm data)
        target-v))))

(defn is-press-release
  [value]
  (= value "PRESS-RELEASE"))

(defn is-not-press-release
  [value]
  (not (= value "PRESS-RELEASE")))

(defn always-true
  [value]
  true)


(defn write-csv-quoted
  [coll file & {:keys [append encoding]}]
  (let [keys-vec (keys (first coll))
        vals-vecs (map (apply juxt keys-vec) coll)]
    (with-open [out (io/writer file :append append :encoding encoding)]
      (binding [*out* out]
        (when-not append
          (print (clj-csv/write-csv (vector (map name keys-vec)) :force-quote true)))
        (doseq [v vals-vecs]
          (let [v (map str v)]
            (print (clj-csv/write-csv (vector v) :force-quote true))))))))

(defn dump-mongo-to-csv
  [db coll file]
  (let [conn (mg/connect)
        db (mg/get-db conn db)]
    (write-csv-quoted (mc/find-maps db coll)
                      file)))

(def rating-search (partial read-into-memory "finance" "debt_rating"))
(def mr-c (dissoc (rating-search :mr :rating) ""))
(def sp-c (dissoc (rating-search :sp :rating) ""))
(def fitch-c (dissoc (rating-search :fitch :rating) ""))

(defn integrate-rating
  [[sp mr fitch]]
  (let [func #(not (or (= % "") (nil? %) (= % "NR")))
        f2 #(= % "NR")]
    (cond
      (func sp) (get sp-c sp)
      (func mr) (get mr-c mr)
      (func fitch) (get fitch-c fitch)
      (f2 sp) (get sp-c sp)
      (f2 mr) (get mr-c mr)
      (f2 fitch) (get fitch-c fitch))))


;;;;;;;;;;;;; save data to mongo ;;;;;;;;;

;(write-first-to-mongo "finance" "rpus_rv")
;(unique-index "finance" "rpus_rv" :zz)
;(update-to-mongo "finance" "rpus_rv" :zz)

#_(write-to-mongo-in-batch
    "finance" "index"
    (lazy-read-csv-head-on "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\index.csv"))

#_(write-to-mongo-in-batch
  "finance" "index2"
  (lazy-read-csv-head-on "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\index2.csv"))


#_(write-to-mongo-in-batch
    "finance" "fisd"
    (lazy-read-csv-head-on "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\fisd.csv"))

#_(write-to-mongo-in-batch
    "finance" "dsenames"
    (lazy-read-csv-head-on "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\dsenames.csv"))


;;;;;;;;;;;;;;;;;;; prepare data ;;;;;;;;;;;;;;;;;

; index, permno, date

#_(ensure-index "finance" "rpus_rv"
                :timestamp_utc 1)

#_(ensure-index "finance" "rpus_rv"
              :news_type 1)

#_(convert-date "finance" "rpus_rv"
                :timestamp_utc :date
                :zz)

#_(convert-variable "finance" "rpus_rv"
                    :isin :permno
                    :zz
                    match-permno)

#_(convert-variable "finance" "rpus_rv"
                  :isin :permno
                  :zz
                  match-isin-permno)


#_(ensure-index "finance" "rpus_rv"
              :permno 1
              :date 2)

#_(convert-date "finance" "fisd"
              :offering_date :date
              :_id)

#_(convert-variable "finance" "fisd"
                  :isin :permno_new
                  :_id
                  match-permno)

#_(convert-variable "finance" "fisd"
                  :cusip :permno_new
                  :_id
                  match-cusip-permno)

#_(convert-variable "finance" "fisd"
                  :isin :permno_new
                  :_id
                  match-isin-permno)


;;;;;;;;;;;;;;;;;;;;; match using query-data-span ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
#_(def sentiment
  (query-date-span
    "finance" "rpus_rv"
    :date :permno))

#_(def ess-before-30-median
  (sentiment
    :ess -30 -1 median))

#_(convert-variables
  "finance" "fisd"
  [:date :permno_new] :ess_before_30_median
  :_id ess-before-30-median)

;;;;;;;;;;;;;;;;;;;; match using query-cond-date-span ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def sentiment
  (query-cond-date-span
    "finance" "rpus_rv"
    :date :permno))

;;;;;;;;;;;;;;;;;;;;;;;;;  press release, 30 days,


(def filter-func
  (filter-by-other-conds is-press-release))


(def the-filter
  (sentiment [:rp_story_id :news_type] filter-func))        ;[unique-v filter-v]


(def count-before-30
  (the-filter :zz -30 -1 count))

(def ess-before-30-median
  (the-filter :ess -30 -1 median))

(def ess-before-30-mean
  (the-filter :ess -30 -1 mean))

(convert-variables
  "finance" "fisd"
  [:date :permno_new] :news_b30_pr
  :_id count-before-30)

(convert-variables
  "finance" "fisd"
  [:date :permno_new] :senti_ess_b30_mean_pr
  :_id ess-before-30-mean)

(convert-variables
  "finance" "fisd"
  [:date :permno_new] :senti_ess_b30_median_pr
  :_id ess-before-30-median)


;;;;;;;;;;;;;;; general function combine all conditons

(defn start-all
  [db coll]
  (let [method {"count"  count
                "median" median
                "mean"   mean}
        news-type {"pr"  is-press-release
                   "npr" is-not-press-release
                   "all" always-true}
        time {"5"  -5
              "10" -10
              "15" -15
              "20" -20
              "25" -25
              "30" -30
              "45" -45
              "60" -60}]
    (doseq [range ["5" "10" "15" "20" "25" "30" "45" "60"]
            target ["ess" "aes"]
            type ["pr" "npr" "all"]
            stat ["count" "median" "mean"]]
      (let [filter-func (filter-by-other-conds (get news-type type))
            the-filter (sentiment [:rp_story_id :news_type] filter-func)
            before (the-filter (keyword target) (get time range) -1 (get method stat))]
        (convert-variables
          db coll
          [:date :permno_new] (keyword (str target "_" stat "_b" range "_" type))
          :_id
          before)))))

(start-all "finance" "fisd")




(dump-mongo-to-csv "finance" "fisd"
                   "C:\\Users\\Xue\\workspace\\econometric_stata\\raw_data\\fisd_matched.csv")

(convert-variables
  "finance" "fisd"
  [:rating_sp :rating_mr :rating_fitch] :rating
  :_id
  integrate-rating)




