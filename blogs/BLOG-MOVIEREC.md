# EXPLAIN, Adapt, Repeat: Hardening a Real Recommendation Workload

We asked for ten movie recommendations.

What we got back was a graph reality check: multi-hop traversals, fanout, filters, aggregation, ranking, and all the tiny planner decisions that quietly decide whether a query feels instant or annoying. Friendly domain, sharp edges.

Movie recommendations are a delightfully sneaky workload. They look simple on the surface, but under the hood they are a stress test wearing a charming jacket.

The point was never just to run a graph query and call it done. The point was to let a real workload expose optimizer gaps, use EXPLAIN to make those gaps legible, and feed those field findings back into graph database development.

This is the story of taking a recommendation query, finding where it was not yet clever enough, and using that evidence to improve both the engine and the developer experience.

## Why movie recommendations?

A recommendation query has a nice shape for a graph database:

- start from a known user,
- walk shared relationships to similar users,
- score candidate items,
- sort and limit the result set.

That structure is familiar, but it is also demanding. It combines fanout, filters, aggregation, ordering, and repeated traversal patterns. Translation: it is the kind of query that politely smiles at your planner while quietly checking whether it knows what it is doing.

For VitalEdge, that makes movie recommendations a strong proving ground. It is concrete enough to be relatable, but complex enough to show the difference between a query that merely runs and a query that is actually well planned.

## The query

The recommendation query we used follows a common pattern:

- find the target user,
- find shared items and nearby peers,
- filter for meaningful similarity,
- rank candidate movies,
- return the top results.

The important part is not the exact schema, but the shape of the work. This query naturally invites multiple execution strategies, which makes it ideal for observing planner behavior.

## The first pass: useful, but not yet optimal

The first version of the query worked correctly, but it exposed the sort of tradeoffs that matter in real systems:

- some parts of the traversal were selective,
- some were broad,
- some predicates could be pushed down,
- others had to remain as residual filters,
- and the planner needed to choose access paths carefully.

This is where EXPLAIN became indispensable. Instead of treating the query as a black box and hoping for the best, we could inspect the physical plan, cardinality estimates, index decisions, and runtime-style diagnostics. That made the optimization conversation concrete.

When a query is not yet optimized, the useful question is not just "why is it slow?" It is also:

- what shape did the planner infer,
- which paths were considered,
- where was selectivity guessed rather than observed,
- and what evidence would justify a better plan next time?

## The loop: query, observe, refine

The key insight from this example is that graph database development works best when it is driven by a loop:

1. Write a real query.
2. Measure how the planner handles it.
3. Identify where the plan is too broad or too conservative.
4. Feed those observations back into the engine.
5. Re-run EXPLAIN and compare the result.

That loop is more valuable than a one-time benchmark because it turns field findings into engine improvement. The query teaches the engine something, the engine reflects it back, and suddenly the whole thing stops feeling theoretical.

In this case, the recommendation workload surfaced a few hard-earned ideas:

- bounded probing is safer than assuming every index is worth using,
- selectivity gates matter for broad graph traversals,
- runtime feedback should not disappear after execution,
- and EXPLAIN should reflect what the engine has learned, not just what it guessed.

## What improved in VitalEdge

This recommendation example drove a series of improvements in executor behavior and planner visibility:

- stage2 adaptive pushdown was introduced with guardrails,
- fast-path runtime counters became visible to developers,
- EXPLAIN gained feedback-aware cost refinement,
- and the payload was normalized so the same facts appear in both flat and grouped form.

That last point matters more than it might first appear. When EXPLAIN is coherent, it is easier to understand the story of a query. And honestly, that is the whole game: make the system readable enough that humans do not have to become archaeologists.

For example, the plan can now show:

- the execution path,
- cardinality entries,
- cost estimate components,
- index decisions,
- and fast-path eligibility.

It now also groups that same information into nested `assessment` blocks. That makes the output easier to read, easier to share with collaborators, easier to consume in tooling, and easier to use as a debugging artifact.

## What the example teaches

This movie recommendation workflow teaches something broader than just one optimization:

- Graph workloads are often exploratory, not neatly synthetic.
- The first working query is rarely the final performance shape.
- EXPLAIN becomes more useful when it tells a narrative, not just a list of operators.
- Runtime observations should feed back into planning and diagnostics.
- Field findings are not just operations noise; they are product signals.

That is the development style we want for VitalEdge. A real query should be able to improve the engine that serves it.

## Closing thought

The best graph database improvements usually come from real queries that refuse to stay simple. This movie recommendation workload did exactly that. It gave us a query that was useful, a plan that was inspectable, and a feedback loop that turned observations into engine improvements.

It is also a clear example of how VitalEdge is meant to evolve: by listening carefully to the shape of the work, one slightly stubborn query at a time.
