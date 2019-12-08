(ns kafka.streams
  (:require [clojure.string :as str])
  (:import [org.apache.kafka.streams KafkaStreams StreamsBuilder StreamsConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams.kstream KStream ForeachAction]))

(defn print-each-foo-record []
  (let [builder (StreamsBuilder.)]
    (-> builder
        (.stream "foo")
        (.foreach (reify
                    ForeachAction
                    (apply [this k v]
                      (prn k v)))))
    builder))

(n-grams* "clojure rocks")
(defn n-grams* [s]
  (->> (.length s)
       (range)
       (map (fn [len]
              (->> (partition (inc len) 1 s)
                   (map str/join))))))


(defn n-gram []
  (let [builder (StreamsBuilder.)]
    (-> builder
        (.stream "foo")
        (.foreach (reify
                    ForeachAction
                    (apply [this k v]
                      (prn k v)))))
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
  (.close stream)

  )
