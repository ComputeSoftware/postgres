(ns computesoftware.postgres
  "Postgres helper functions."
  (:require
    [clojure.spec.alpha :as s]
    [computesoftware.postgres.credentials :as credentials]
    [computesoftware.postgres.format :as format]
    [next.jdbc :as jdbc])
  (:import
    (java.sql ResultSet)
    (java.util Calendar TimeZone)
    (javax.sql DataSource)))

(set! *warn-on-reflection* true)

(s/def ::host string?)
(s/def ::dbname string?)

(s/def ::db-spec
  (s/keys :req-un [::host
                   ::dbname]))

(s/def ::user string?)
(s/def ::password? string?)

(s/def ::properties
  (s/merge
    (s/keys :opt-un [::user ::password])
    (s/map-of
      (s/or :keyword keyword? :string string?) string?)))

(s/def ::credentials-provider #(satisfies? credentials/Provider %))

(s/def ::connection-factory-argm
  (s/keys
    :req-un [::db-spec]
    :opt-un [::properties
             ::credentials-provider]))

(defn connection-factory*
  "Returns a thunk that returns Connections. Takes an argument map of the following.
    :db-spec
    :properties
    :credentials-provider"
  [{:keys [db-spec
           properties
           credentials-provider]}]
  (let [init-props (doto (java.util.Properties.)
                     (.putAll (merge
                                (cond-> {}
                                  (:user db-spec) (assoc "user" (:user db-spec))
                                  (:password db-spec) (assoc "password" (:password db-spec)))
                                properties)))
        db-url (format "jdbc:postgresql://%s/%s" (:host db-spec) (:dbname db-spec))
        get-conn (fn create!
                   ([] (create! nil))
                   ([{:keys [properties]}]
                    (java.sql.DriverManager/getConnection db-url
                      (doto init-props
                        (.putAll (or (some-> credentials-provider (credentials/fetch)) {}))
                        (.putAll (or properties {}))))))]
    {:get-conn get-conn
     :db-url   db-url}))

(s/fdef connection-factory*
  :args (s/cat :argm ::connection-factory-argm)
  :ret map?)

(defn connection-factory
  [connection-factory-argm]
  (:get-conn (connection-factory* connection-factory-argm)))

(s/fdef connection-factory
  :args (s/cat :argm ::connection-factory-argm)
  :ret fn?)

(defn get-datasource
  "Returns a DataSource given the connection-factory argm."
  [connection-factory-argm]
  (let [{:keys [get-conn db-url]} (connection-factory* connection-factory-argm)
        *login-timeout (atom nil)]
    (reify DataSource
      (getConnection [_] (get-conn))
      (getConnection [_ user password]
        (get-conn {:properties {"user"     user
                                "password" password}}))
      (getLoginTimeout [_] (or @*login-timeout 0))
      (setLoginTimeout [_ seconds] (reset! *login-timeout seconds))
      (toString [_] db-url))))

(comment
  (get-datasource {:db-spec {:host   "localhost"
                             :dbname "test"}}))

(defn get-connection
  [argm]
  ((connection-factory argm)))

(defn sql-timestamp
  "Returns a string constant cast as a Timestamp."
  [sql-timestamp-like]
  (format/-format sql-timestamp-like))

(defn sql-ident
  "Returns an escaped String formatted as a SQL delimited identifier or quoted
  identifier, formed by enclosing an arbitrary string in double-quotes.

  https://www.postgresql.org/docs/9.4/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS"
  [^String s & etc]
  (apply format/sql-ident s etc))

(defn sql-constant
  "Returns an escaped String formatted as a SQL string constant, formed by
  enclosing an arbitrary string in single-quotes.

  https://www.postgresql.org/docs/9.4/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS"
  [^String s & etc]
  (apply format/sql-constant s etc))

(defn sql-cast-constant
  "Returns a string constant cast as a Timestamp."
  [constant type]
  (format/sql-cast (format/sql-constant constant) type))

(defn ensure-ident-fn
  "Returns a function that given a string, returns a new string that is less
  than max-identifier-length in length and uniquely identifiable from other
  strings >= max-identifier-length."
  ([] (ensure-ident-fn nil))
  ([{:keys [max-identifier-length ^java.nio.charset.Charset charset]
     :or   {;; Default max_identifier_length is 63 bytes (not chars)
            ;; https://www.postgresql.org/docs/9.3/runtime-config-preset.html#GUC-MAX-IDENTIFIER-LENGTH
            max-identifier-length 63
            charset               java.nio.charset.StandardCharsets/UTF_8}}]
   (let [*lookup (atom {})
         get! (fn [s]
                (-> *lookup
                  (swap! (fn [lookup]
                           (if (get lookup s)
                             lookup
                             (assoc lookup s (count lookup)))))
                  (get s)))]
     (fn [^String s]
       (let [b (.getBytes s charset)]
         (if (< (alength b) max-identifier-length)
           s
           (let [suffix (.getBytes (str ":" (get! s)) charset)]
             (System/arraycopy suffix 0 b (- max-identifier-length (alength suffix)) (alength suffix))
             (String. b 0 ^int max-identifier-length charset))))))))

(defn str-keyed-resultset-seq
  "Creates and returns a lazy sequence of structmaps corresponding to
  the rows in the java.sql.ResultSet rs"
  [^ResultSet rs]
  (let [rsmeta (. rs (getMetaData))
        mapping (into {}
                  (map (fn [i]
                         (let [i (int i)]
                           [(.getColumnLabel rsmeta i)
                            (case (.getColumnTypeName rsmeta i)
                              "timestamp"
                              (let [cal (Calendar/getInstance (TimeZone/getTimeZone "Z"))]
                                #(.getTimestamp rs i cal))
                              #(.getObject rs i))])))
                  (range 1 (inc (.getColumnCount rsmeta))))
        row-struct (apply create-struct (keys mapping))
        row-values (apply juxt (vals mapping))
        rows (fn thisfn []
               (when (.next rs)
                 (cons (apply struct row-struct (row-values)) (lazy-seq (thisfn)))))]
    (rows)))

(defn set-schema!
  "Sets the search_path to schema."
  [connectable {:keys [schema]}]
  (jdbc/execute! connectable [(str "SET search_path TO" (sql-ident schema) ",public")]))

(defn explain!
  "Print a query plan result.

  If used with jdbc/execute!, ensure `{:builder-fn rs/as-unqualified-maps}` is
  passed in the opts map."
  [result]
  (reduce
    (fn [out r]
      (if-let [p (get r (keyword "QUERY PLAN"))]
        (do
          (println p)
          (drop 1 out))
        (reduced out)))
    result result))
