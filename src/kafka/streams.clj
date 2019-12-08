(ns kafka.streams
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.streams KafkaStreams  KeyValue StreamsBuilder StreamsConfig ]
           [org.apache.kafka.streams.state Stores QueryableStoreTypes]
           [org.apache.kafka.streams.kstream Materialized]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams.kstream KStream ForeachAction KeyValueMapper]))

(defn print-each-foo-record []
  (let [builder (StreamsBuilder.)]
    (-> builder
        (.stream "foo")
        (.foreach
         (reify ForeachAction
           (apply [this k v]
             (prn k v)))))
    builder))

(defn n-grams* [s]
  (->> (.length s)
       (range)
       (map (fn [len]
              (->> (partition (inc len) 1 s)
                   (map str/join))))))

(defn n-gram []
  (let [builder (StreamsBuilder.)]
    (-> builder
        (.stream "text")
        (.flatMap (reify KeyValueMapper
                    (apply [this k v]
                      (->> (n-grams* v)
                           (flatten)
                           (map #(KeyValue. (str (count %)) %))))))
        (.peek
         (reify ForeachAction
           (apply [this k v]
             (prn k v))))
        (.to "grams"))
    builder))

(defn ngram-count-table []
  (let [builder (StreamsBuilder.)]
    (-> builder
        (.stream "grams")
        (.groupByKey)
        (.count (Materialized/as "count-table")))
    builder))


(comment
  (def config {})

  (def stream
    (doto (KafkaStreams.
           (.build (print-each-foo-record))
           (StreamsConfig. {"bootstrap.servers" "localhost:9092"
                            "application.id" "my-app"
                            "default.key.serde" (.getClass (Serdes/String))
                            "default.value.serde" (.getClass (Serdes/String))}))
      (.start)))

  (def stream
    (doto (KafkaStreams.
           (.build (n-gram))
           (StreamsConfig. {"bootstrap.servers" "localhost:9092"
                            "application.id" "my-app"
                            "default.key.serde" (.getClass (Serdes/String))
                            "default.value.serde" (.getClass (Serdes/String))}))
      (.start)))

  (.close stream)

  (def count-table-stream
    (doto (KafkaStreams.
           (.build (ngram-count-table))
           (StreamsConfig. {"bootstrap.servers" "localhost:9092"
                            "application.id" "ngram-count"
                            "default.key.serde" (.getClass (Serdes/String))
                            "default.value.serde" (.getClass (Serdes/String))}))
      (.start)))

  (.close count-table-stream)

  (.get (.store count-table-stream
                "count-table"
                (QueryableStoreTypes/keyValueStore))
        "900"))
