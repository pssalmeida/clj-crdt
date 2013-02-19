
(ns
  ^{:doc "Counter crdt that allows increment operations.
          Allows multi-tier hierarchies of nodes with efficient ID management."
    :author "Paulo Sérgio Almeida"}
  crdt.handoff-counter)

(defn init [id tier]
  "Creates starting state for a node.
   Must be invoked only once for each globally unique ID.
   Depth is 0 for a root server (e.g., inter-datacenter communication nodes),
   1 for the lower tier servers they connect to (e.g., ring in datacenter),
   2 for the next tier servers, clients are not connected to lower ids then themselves."
  {:id id
   :tier tier
   :val 0
   :below 0
   :sck 0
   :dck 0
   :slots (sorted-map)
   :tokens (sorted-map)
   :vals (assoc (sorted-map) id 0)})

(defn value [counter]
  "Counter value."
  (:val counter))

(defn incr [counter]
  "Increments counter."
  (-> counter
    (update-in [:val] inc)
    (update-in [:vals (:id counter)] inc)))

(defn- discard-tokens [{tokens :tokens :as counter} {id2 :id dc2 :dck slots2 :slots}]
  (assoc counter :tokens
    (conj (sorted-map)
          (remove
            (fn [[[src dst] {dc :dck}]]
              (and (= dst id2)
                   (if (slots2 src)
                     (> (:dck (slots2 src)) dc)
                     (>= dc2 dc))))
            tokens))))

(defn- discard-slot [{id :id slots :slots :as counter} {id2 :id sc2 :sck tokens2 :tokens}]
  (if (and (slots id2)
           (not (tokens2 [id2 id]))
           (> sc2 (:sck (slots id2))))
    (assoc counter :slots (dissoc slots id2))
    counter))

(defn- fill-slots [{id :id tier :tier vs :vals slots :slots :as counter}
                   {id2 :id tier2 :tier sc2 :sck tokens2 :tokens}]
  (let [S (for [[[src dst] {dct :dck n :val}] tokens2
                :when (and (= dst id)
                           (= dct (:dck (slots src))))]
            [src n])]
    (assoc counter
           :vals (assoc vs id (apply + (vs id) (map second S)))
           :slots (apply dissoc slots (map first S)))))

(defn- merge-vectors [{tier :tier v :vals :as counter} {tier2 :tier v2 :vals}]
  (if (= tier tier2 0)
    (assoc counter :vals (merge-with max v v2)) 
    counter))

(defn- cache-values [{id :id tier :tier v :val vs :vals below :below :as counter}
                         {id2 :id tier2 :tier v2 :val vs2 :vals below2 :below}]
  (let [b' (cond
             (= tier tier2) (max below below2)
             (> tier tier2) (max below v2)
             true           below)
        v' (cond
             (zero? tier)   (apply + (vals vs))
             (= tier tier2) (max v v2 (+ b' (vs id) (vs2 id2)))
             true           (max v (+ b' (vs id))))]
    (assoc counter :below b' :val v')))

(defn- create-slot [{tier :tier dc :dck slots :slots :as counter}
                    {tier2 :tier id2 :id sc2 :sck v2 :vals}]
  (if (and (< tier tier2)
           (pos? (v2 id2))
           (or (not (slots id2))
               (> sc2 (:sck (slots id2)))))
    (assoc-in (assoc counter :dck (inc dc)) [:slots id2] {:sck sc2 :dck (inc dc)})
    counter))

(defn- create-token [{id :id sc :sck vs :vals :as counter} {id2 :id slots2 :slots}]
  (if-let [{scs :sck dcs :dck} (slots2 id)]
    (if (= scs sc)
      (-> counter
        (assoc-in [:tokens [id id2]] {:sck scs :dck dcs :val (vs id)})
        (assoc-in [:vals id] 0)
        (assoc :sck (inc sc)))
      counter) 
    counter))

(defn- cache-tokens [{id :id tier :tier tokens :tokens :as counter} {id2 :id tier2 :tier tokens2 :tokens}]
  (if (< tier tier2)
    (assoc counter :tokens
           (merge-with (fn [{sc1 :sck :as tok1} {sc2 :sck :as tok2}] (if (>= sc1 sc2) tok1 tok2))
                       tokens
                       (filter (fn [[[src dst] _]] (and (= src id2) (not= dst id))) tokens2))) 
    counter))

(defn join
  "Joins counter2 into counter, returning updated counter."
  [counter counter2]
  (if (= (:id counter) (:id counter2))
    counter
    (reduce #(%2 %1 counter2)
            counter
            [discard-tokens
             discard-slot
             fill-slots
             merge-vectors
             cache-values
             create-slot
             create-token
             cache-tokens])))

