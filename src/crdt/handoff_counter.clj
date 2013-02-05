
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
   :above 0
   :src-clock 0
   :dst-clock 0
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

(defn- merge-vectors [{tier :tier v :vals :as counter} {tier2 :tier v2 :vals}]
  (if (and (zero? tier) (zero? tier2))
    (assoc counter :vals (merge-with max v v2)) 
    counter))

(defn- discard-tokens [{tokens :tokens :as counter} {id2 :id dc2 :dst-clock slots2 :slots}]
  (assoc counter :tokens
    (conj (sorted-map)
          (remove
            (fn [[[src dst] {dc :dst-clock}]]
              (and (= dst id2)
                   (if (slots2 src)
                     (> (:dst-clock (slots2 src)) dc)
                     (>= dc2 dc))))
            tokens))))

(defn- cache-tokens [{id :id tier :tier tokens :tokens :as counter} {id2 :id tier2 :tier tokens2 :tokens}]
  (if (< tier tier2)
    (assoc counter :tokens
           (merge-with (fn [{sc1 :src-clock :as tok1} {sc2 :src-clock :as tok2}] (if (>= sc1 sc2) tok1 tok2))
                       tokens
                       (filter (fn [[[src dst] _]] (and (= src id2) (not= dst id))) tokens2))) 
    counter))

(defn- discard-slot [{id :id slots :slots :as counter} {id2 :id sc2 :src-clock tokens2 :tokens}]
  (if (and (slots id2)
           (not (tokens2 [id2 id]))
           (> sc2 (:src-clock (slots id2))))
    (assoc counter :slots (dissoc slots id2))
    counter))

(defn- fill-slots [{id :id tier :tier vs :vals slots :slots :as counter}
                   {id2 :id tier2 :tier sc2 :src-clock tokens2 :tokens}]
  (let [[vid slots]
        (reduce
          (fn [[vid slots] [[src dst] {dct :dst-clock n :val}]]
            (if (= dst id)
              (if-let [{dcs :dst-clock} (slots src)]
                (if (= dct dcs)
                  [(+ vid n) (dissoc slots src)]
                  [vid slots])
                [vid slots])
              [vid slots]))
          [(vs id) slots]
          tokens2)]
    (assoc counter :vals (assoc vs id vid) :slots slots)))

(defn- create-slot [{tier :tier dc :dst-clock slots :slots :as counter}
                    {tier2 :tier id2 :id sc2 :src-clock v2 :vals}]
  (if (and (< tier tier2)
           (pos? (v2 id2))
           (or (not (slots id2))
               (> sc2 (:src-clock (slots id2)))))
    (assoc-in (assoc counter :dst-clock (inc dc)) [:slots id2] {:src-clock sc2 :dst-clock (inc dc)})
    counter))

(defn- create-token [{id :id sc :src-clock vs :vals :as counter} {id2 :id slots2 :slots}]
  (if-let [{scs :src-clock dcs :dst-clock} (slots2 id)]
    (if (= scs sc)
      (let [n (vs id)]
        (-> counter
          (assoc-in [:tokens [id id2]] {:src-clock scs :dst-clock dcs :val n})
          (assoc-in [:vals id] 0)
          (assoc :src-clock (inc sc))))
      counter) 
    counter))

(defn- update-estimates [{tier :tier v :val vs :vals above :above :as counter}
                         {tier2 :tier v2 :val above2 :above}]
  (let [above (cond
                (= tier tier2) (max above above2)
                (> tier tier2) (max above v2)
                true           above)]
    (assoc counter
           :above above
           :val (max (if (= tier tier2) (max v v2) v) (apply + above (vals vs))))))

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
             update-estimates
             create-slot
             create-token
             cache-tokens])))

