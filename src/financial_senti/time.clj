(ns financial-senti.time
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clojure.string :as s])
  (:import [java.util Locale]
           [org.joda.time Chronology DateTime DateTimeZone Interval
                          LocalDateTime Period PeriodType LocalDate LocalTime]
           [org.joda.time.format DateTimeFormat DateTimeFormatter
                                 DateTimePrinter DateTimeFormatterBuilder
                                 DateTimeParser ISODateTimeFormat]))

(def custom-formatter
  (f/formatter "ddMMMyyyy"))

(defn parse-date
  [string]
  (-> custom-formatter
      (.withLocale Locale/ENGLISH)
      (.parseLocalDate string)
      .toDate))


(defn date-span
  [start end today]
  (let [today (c/from-date today)
        start (if (> start 0)
                (t/plus today (t/days start))
                (t/minus today (t/days (- start))))
        end (if (> end 0)
              (t/plus today (t/days end))
              (t/minus today (t/days (- end))))]
    (mapv #(.toDate %) [start end])))

;(parse-date "10Mar2005")
;(date-span 0 30 (parse-date "10Mar2005"))
(date-span -30 -1 (parse-date "10Mar2005"))
