(ns computesoftware.postgres.format)

(set! *warn-on-reflection* true)

(defn sql-escape [ch]
  (let [ch (char ch)
        ich (int ch)]
    (fn esc
      ([s & etc] (esc (apply str s etc)))
      ([^String s]
       (let [sb (StringBuilder.)]
         (loop [from 0]
           (.append sb ch)
           (let [i (.indexOf s ich from)]
             (if (neg? i)
               (doto sb
                 (.append (.substring s from))
                 (.append ch))
               (let [i (inc i)]
                 (.append sb (.substring s from i))
                 (recur i)))))
         (.toString sb))))))

(def ^String sql-ident (sql-escape \"))
(def ^String sql-constant (sql-escape \'))

(defn sql-cast
  [x type]
  (str x "::" (name type)))

(defn ^String sql-timestamp* [date-iso-string]
  (sql-cast (sql-constant date-iso-string) "timestamp"))

(defprotocol ISqlTimestamp
  (-format [_]))

(extend-protocol ISqlTimestamp
  java.util.Date
  (-format [date]
    (->> date .toInstant (.format java.time.format.DateTimeFormatter/ISO_INSTANT) sql-timestamp*))

  java.time.LocalDate
  (-format [local-date]
    (-> local-date str sql-timestamp*))

  java.time.LocalDateTime
  (-format [local-date-time]
    (sql-timestamp* (.format local-date-time java.time.format.DateTimeFormatter/ISO_DATE_TIME)))

  java.time.YearMonth
  (-format [year-month]
    (-> year-month (.atDay 1) -format))

  java.time.Year
  (-format [year]
    (-> year (.atMonth 1) -format)))
