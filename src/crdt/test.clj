(ns crdt.test
  (:use crdt.handoff-counter))

(def PAST 5)

(defn prn-env [env]
  (println "State:")
  (doseq [x (sort (keys env))]
    (println x (peek (env x)))))

(def mx (atom 0)) 
(def incrs (atom 0)) 

(defn t-op [fsym & args]
  (fn [env]
    (let [f (eval fsym)
          a (first args)]
      (do
        (cond
          (= f init) 
          (do
            (assert (not (env a)))
            (assoc env a (mapv (constantly (init a (second args))) (range PAST))))
          true
          (let [res (apply f (peek (env a)) (rest args))
                _   (swap! mx max (value res))
                _ (assert (<= (value res) @incrs) (str (value res) " " @incrs))
                ]
            (assoc env a (conj (subvec (env a) 1) res))))))))

(defn vars [prefix col]
  (into [] (map symbol (map #(str prefix %) col))))

(defn run []
(let [_  (reset! mx 0)
      _  (reset! incrs 0)
      clients (vars "c" (range 10))
      servers2 (vars "s2" (range 6))
      servers1 (vars "s1" (range 4))
      roots (vars "d" (range 2))
      nodes   (concat clients servers2 servers1 roots)
      rand-op (fn [env] 
                (let [o (rand-nth '[incr join])]
                  (case o 
                    incr    (do (swap! incrs inc) ((t-op 'incr (rand-nth nodes)) env)) 
                    join    ((t-op 'join (rand-nth nodes) (rand-nth (env (rand-nth nodes)))) env))))
      env (reduce (fn [env node] ((t-op 'init node 3) env)) {} clients)
      env (reduce (fn [env node] ((t-op 'init node 2) env)) env servers2)
      env (reduce (fn [env node] ((t-op 'init node 1) env)) env servers1)
      env (reduce (fn [env node] ((t-op 'init node 0) env)) env roots)
      env (reduce (fn [env _] (rand-op env)) env (range 200000))
      env (reduce (fn [env i] ((t-op 'join (rand-nth nodes) (rand-nth (env (rand-nth nodes)))) env)) env (range 20000))
      ]
  (prn-env env)
  (println "mx: " @mx)
  (println "inc:" @incrs)
  (doseq [n (sort (keys env))] (println "val:" (value (peek (env n))))) 
  )) 

(time (run)) 

