(ns computesoftware.postgres.credentials)

(defprotocol Provider
  (-fetch [provider]))

(defn fetch
  "Fetch credentials for a connection. Returns a string keyed map to be included
   in the Properties passed to getConnection."
  [provider]
  (-fetch provider))

(defn identity-provider
  [x]
  (reify Provider (-fetch [_] x)))
