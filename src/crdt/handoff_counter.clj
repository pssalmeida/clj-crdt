
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

(defn- discard-tokens [{id :id tokens :tokens} {id2 :id dc2 :dst-clock slots2 :slots}]
  (conj (sorted-map)
        (remove
          (fn [[[src dst] {dc :dst-clock}]]
            (and (= dst id2)
                 (if (slots2 src)
                   (> (:dst-clock (slots2 src)) dc)
                   (>= dc2 dc))))
          tokens)))

(defn- cache-tokens [id tier tokens {id2 :id tier2 :tier tokens2 :tokens}]
  (if (< tier tier2)
    (merge-with (fn [{sc1 :src-clock :as tok1} {sc2 :src-clock :as tok2}] (if (>= sc1 sc2) tok1 tok2))
                tokens
                (filter (fn [[[src dst] _]] (and (= src id2) (not= dst id))) tokens2))
    tokens))

(defn- acquire-tokens [id tier vid slots {id2 :id tier2 :tier sc2 :src-clock tokens2 :tokens}]
  (reduce
    (fn [[vid slots] [[src dst] {dct :dst-clock n :val}]]
      (if (= dst id)
        (if-let [{dcs :dst-clock} (slots src)]
          (if (= dct dcs)
            [(+ vid n) (dissoc slots src)]
            [vid slots])
          [vid slots])
        [vid slots]))
    (if (and (slots id2) (not (tokens2 [id2 id])) (> sc2 (:src-clock (slots id2)))) 
      [vid (dissoc slots id2)]
      [vid slots])
    tokens2))

(defn- create-slot [tier dst-clock slots {tier2 :tier id2 :id sc2 :src-clock v2 :vals}]
  (if (and (< tier tier2)
           (pos? (v2 id2))
           (or (not (slots id2))
               (> sc2 (:src-clock (slots id2)))))
    [(inc dst-clock) (assoc slots id2 {:src-clock sc2 :dst-clock (inc dst-clock)})]
    [dst-clock slots]))

(defn- create-token [{id :id sc :src-clock vs :vals :as counter} id2 slots2]
  (if-let [{scs :src-clock dcs :dst-clock} (slots2 id)]
    (if (= scs sc)
      (let [n (vs id)]
        (-> counter
          (assoc-in [:tokens [id id2]] {:src-clock scs :dst-clock dcs :val n})
          (assoc-in [:vals id] 0)
          (assoc :src-clock (inc sc))))
      counter) 
    counter))

(defn above [{tier1 :tier above1 :above} {tier2 :tier val2 :val above2 :above}]
  (cond
    (= tier1 tier2) (max above1 above2)
    (> tier1 tier2) (max above1 val2)
    true            above1))

(defn join
  "Joins counter2 into counter1, returning updated counter1."
  [{id1 :id tier1 :tier v1 :vals slots1 :slots t1 :tokens :as counter1}
   {id2 :id tier2 :tier v2 :vals slots2 :slots t2 :tokens :as counter2}]
  (if (= id1 id2)
    counter1
    (let [v           (if (and (zero? tier1) (zero? tier2)) (merge-with max v1 v2) v1)
          toks        (discard-tokens counter1 counter2)
          toks        (cache-tokens id1 tier1 toks counter2)
          [vid slots] (acquire-tokens id1 tier1 (v1 id1) slots1 counter2)
          [dc slots]  (create-slot tier1 (:dst-clock counter1) slots counter2)  
          counter     (assoc counter1 :vals (assoc v id1 vid) :dst-clock dc :slots slots :tokens toks)
          counter     (create-token counter id2 slots2)
          above1      (above counter1 counter2)]
      (assoc counter
             :val (max (:val counter1) (:val counter2) (apply + above1 (vals (:vals counter))))
             :above above1))))

