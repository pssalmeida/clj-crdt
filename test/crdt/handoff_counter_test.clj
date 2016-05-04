(ns crdt.handoff-counter-test
  (:use clojure.test
        crdt.handoff-counter))

(def PAST 3)

(def mx (atom 0))
(def incrs (atom 0))

(defmacro rand-expr [& exprs]
  (let [n (count exprs)
        v (range n)
        pairs (interleave v exprs)]
    `(case (rand-int ~n) ~@pairs)))

(defn vars [prefix col]
  (into [] (map symbol (map #(str prefix %) col))))

(defn t-init [node tier]
  (fn [env]
     (do (assert (not (env node)))
         (assoc env node (mapv (constantly (init node tier)) (range PAST))))))

(defn t-op [f node & args]
  (fn [env]
    (let [res (apply f (peek (env node)) args)
          _   (swap! mx max (value res))
          _ (assert (<= (value res) @incrs) (str (value res) " " @incrs))
          ]
      (assoc env node (conj (vec (subvec (env node) 1)) res)))))

(defn rand-trace [N C S R]
  (let [_  (reset! mx 0)
        _  (reset! incrs 0)
        clients (vars "c" (range C))
        servers (vars "s" (range S))
        roots (vars "r" (range R))
        nodes   (concat clients servers roots)
        rand-op (fn [env]
                  (rand-expr (do (swap! incrs inc) ((t-op incr (rand-nth nodes)) env))
                             ((t-op join (rand-nth nodes) (rand-nth (env (rand-nth nodes)))) env)))
        env (reduce (fn [env node] ((t-init node 2) env)) {} clients)
        env (reduce (fn [env node] ((t-init node 1) env)) env servers)
        env (reduce (fn [env node] ((t-init node 0) env)) env roots)
        env (reduce (fn [env _] (rand-op env)) env (range N))
        ;env (reduce (fn [env i] ((t-op join (rand-nth nodes) (rand-nth (env (rand-nth nodes)))) env)) env (range 20000))
        env (reduce (fn [env i] ((t-op join (rand-nth nodes) (peek (env (rand-nth nodes)))) env)) env (range 20000))
        ]
    env))

(defn correct? [env]
  (apply = @incrs @mx (map #(value (peek (env %))) (keys env))))

(defn prn-env [env]
  (println "State:")
  (doseq [x (sort (keys env))]
    (println x (peek (env x)))))

(defn run [N C S R]
  (let [env (rand-trace N C S R)]
    (prn-env env)
    (println "Correct:" (correct? env))))

(defn -main [N C S R & args]
  (apply run (map read-string [N C S R])))

(deftest handoff-counter-random-trace
  (testing "handoff-counter"
    (testing "random trace"
      (is (correct? (rand-trace 1000000 12 6 3))))))

