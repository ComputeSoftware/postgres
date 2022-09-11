(ns computesoftware.postgres.iam-auth
  "Authenticate to a DB instance using AWS IAM."
  (:require
    [computesoftware.postgres.credentials :as credentials])
  (:import (com.amazonaws.services.rds.auth RdsIamAuthTokenGenerator GetIamAuthTokenRequest)
           (com.amazonaws.auth DefaultAWSCredentialsProviderChain)
           (java.time Duration Instant)
           (com.amazonaws.regions DefaultAwsRegionProviderChain)))

(defn generate-auth-token
  "Create an authorization token used to connect to a database that uses RDS IAM
  authentication. Use this token as the DB password when connecting with `user`.

  To use IAM authentication, the user must be granted the rds_iam role. e.g.,
  `GRANT rds_iam TO db_userx;`

  See also: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html"
  [{:postgres.iam-auth/keys [db-spec
                             region
                             aws-credentials-provider]}]
  (let [aws-credentials-provider (or aws-credentials-provider (DefaultAWSCredentialsProviderChain.))
        region (or region (.getRegion (DefaultAwsRegionProviderChain.)))
        generator (.build
                    (doto (RdsIamAuthTokenGenerator/builder)
                      (.credentials aws-credentials-provider)
                      (.region ^String region)))
        request (.build
                  (doto (GetIamAuthTokenRequest/builder)
                    (.hostname (:host db-spec))
                    (.port (:port db-spec 5432))
                    (.userName (:user db-spec))))]
    (.getAuthToken generator request)))

(comment (.getRegion (DefaultAwsRegionProviderChain.)))

(defn credentials-provider
  "Returns a credentials/Provider that automatically fetches new AWS IAM auth
  tokens as needed."
  [generate-auth-token-argm]
  (let [;; IAM creds have a TTL of 15 minutes. We set the ttl-dur to 1m less to
        ;; ensure we have time to refresh the creds.
        ttl-dur (Duration/ofMinutes 14)
        *cache (atom nil)
        tokenf (fn []
                 (let [token (generate-auth-token generate-auth-token-argm)
                       expiry (.plus (Instant/now) ttl-dur)
                       data {::token  token
                             ::expiry expiry}]
                   (reset! *cache data)
                   data))]
    (reify credentials/Provider
      (-fetch [_]
        (let [{::keys [token]}
              (if-let [{::keys [expiry] :as data} @*cache]
                (if (.isAfter (Instant/now) expiry)
                  (tokenf)
                  data)
                (tokenf))]
          {"password" token})))))
