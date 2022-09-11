(ns computesoftware.postgres.hikari
  "Functions for working with Hikari JDBC connection pooling."
  (:require
    [com.stuartsierra.component :as component]
    [computesoftware.postgres :as postgres])
  (:import (com.zaxxer.hikari HikariDataSource)))

(defn- apply-hikari-properties
  [hikari-datasource hikari-properties]
  (let [{:keys [auto-commit
                connection-timeout
                idle-timeout
                max-lifetime
                maximum-pool-size
                minimum-idle
                read-only
                validation-timeout]} hikari-properties]
    (when (some? auto-commit)
      (.setAutoCommit hikari-datasource auto-commit))
    (when connection-timeout
      (.setConnectionTimeout hikari-datasource connection-timeout))
    (when idle-timeout
      (.setIdleTimeout hikari-datasource idle-timeout))
    (when max-lifetime
      (.setMaxLifetime hikari-datasource max-lifetime))
    (when maximum-pool-size
      (.setMaximumPoolSize hikari-datasource maximum-pool-size))
    (when minimum-idle
      (.setMinimumIdle hikari-datasource minimum-idle))
    (when (some? read-only)
      (.setReadOnly hikari-datasource auto-commit))
    (when validation-timeout
      (.setValidationTimeout hikari-datasource validation-timeout))))

(defn new-data-source
  "Returns a thunk that returns Connections. Takes an argument map of the following.
  The argument map is identical to the one passed to [[postgres/get-datasource]].

    :db-spec - A map containing the following.
      :host
      :dbname

    :properties - Connection parameters (see https://jdbc.postgresql.org/documentation/head/connect.html)

    :credentials-provider - Provider expected to return a password used to connect
      to the db.

    :hikari-properties - A map of properties to apply to the Hikari Datasource.
      (see https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby)

      :auto-commit - Set the default auto-commit behavior of connections in the pool.
        Default: true

      :connection-timeout - Set the maximum number of milliseconds that a client
        will wait for a connection from the pool. If this time is exceeded
        without a connection becoming available, a SQLException will be thrown
        from DataSource.getConnection().
        Default: 30000

      :idle-timeout - This property controls the maximum amount of time
        (in milliseconds) that a connection is allowed to sit idle in the pool.
        Whether a connection is retired as idle or not is subject to a maximum
        variation of +30 seconds, and average variation of +15 seconds. A
        connection will never be retired as idle before this timeout. A value of
        0 means that idle connections are never removed from the pool.
        Default: 600000

      :max-lifetime - This property controls the maximum lifetime of a connection
        in the pool. When a connection reaches this timeout, even if recently
        used, it will be retired from the pool. An in-use connection will never
        be retired, only when it is idle will it be removed.
        Default: 1800000

      :maximum-pool-size - The property controls the maximum size that the pool
        is allowed to reach, including both idle and in-use connections.
        Basically this value will determine the maximum number of actual connections
        to the database backend.
        When the pool reaches this size, and no idle connections are available,
        calls to getConnection() will block for up to connectionTimeout milliseconds
        before timing out.
        (see: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing)
        Default: 10

      :minimum-idle - The property controls the minimum number of idle connections
        that HikariCP tries to maintain in the pool, including both idle and in-use
        connections. If the idle connections dip below this value, HikariCP will
        make a best effort to restore them quickly and efficiently.
        Default: same as maximumPoolSize

      :pool-name: Set the name of the connection pool.
        Default: auto-generated

      :read-only - Configures the Connections to be added to the pool as
        read-only Connections.
        Default: false

      :validation-timeout - Sets the maximum number of milliseconds that the pool
        will wait for a connection to be validated as alive. This value must be
        less than the connectionTimeout.
        Default: 5000"
  [{:keys [hikari-properties]
    :as   argm}]
  (let [base-datasource (postgres/get-datasource argm)
        ;; The no arg ctor will not initialize the data source. The first call
        ;; to getConnection will initialize it.
        data-source (doto (HikariDataSource.)
                      (.setDataSource base-datasource))]
    (apply-hikari-properties data-source hikari-properties)))

(defn new-component
  [{::keys [get-data-source-args]}]
  (with-meta {::data-source nil}
    {`component/start (fn [component]
                        (let [data-source (new-data-source (get-data-source-args component))
                              get-conn #(.getConnection data-source)]
                          ;; Intializes the pool
                          (get-conn)
                          (assoc component
                            ::data-source data-source
                            ::get-connection get-conn)))
     `component/stop  (fn [component]
                        (when-let [data-source (::data-source component)]
                          (.close data-source))
                        {})}))
