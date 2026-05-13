MiddlewareInterface refactoring project
=======================================

The Prompt Processing system started out as just two modules, `activator.py` to handle network dependencies and application logic, and `middleware_interface.py` to handle anything that used Middleware.
As described in [Package organization](organization.md#directory-and-module-organization), it has since grown much larger and more complex.
While there are many Jira work items filed under the `Prompt-processing-refactor` label, developers should be aware of the ongoing refactoring of the `MiddlewareInterface` class ([DM-47742](https://rubinobs.atlassian.net/browse/DM-47742) and sub-tasks) because of its disruptive scale.

Motivation
----------

The `MiddlewareInterface` class originally handled everything involving Middleware -- initializing the local repo and its collections, preloading pipeline inputs, ingesting raws, running the pipeline, sending the results to the central repo, and cleaning up.
As more features were added to Prompt Processing -- configurable pipelines, a preprocessing pipeline, automated identification of inputs, raw image verification, and many more -- any feature that involved the Butler or pipelines was added to `MiddlewareInterface`.
The result was a class that, at its peak, was 1640 lines long, had 50 methods and 21 fields, and depended on 26 other packages (not counting the Python standard library).
The two test suites in `test_middlewareinterface.py` were likewise bloated.
While the public interface, aside from the 10-parameter constructor, was still simple, it was difficult to understand what the class did (what, exactly, does a `MiddlewareInterface` object represent?) and how its pieces were interrelated.

The fundamental mistake made with `MiddlewareInterface` was the assumption that all related functionality must be part of a single class (a multifaceted abstraction, in the terminology of [Suryanarayana2015]).
The same class managed the local repo, sent data to and from the central repo (operations unrelated except for both calling `Butler.transfer_from`), queried both repos, ingested raws (not using the `transfer_from` mechanism), and selected and executed pipelines.
Of the 21 fields, many were only needed for one of these responsibilities, while others, such as `visit`, affected several at a time.
The result was that modifying `MiddlewareInterface` required familiarity with the class as a whole to avoid side effects or duplicated functionality.

A related issue was that the `middleware_interface` module had become a "hub" module (term from [Suryanarayana2015]) that used or was used by most of the other modules in Prompt Processing.
This meant that most changes to Prompt Processing needed to touch `middleware_interface` (or vice versa), and its unit tests also depended on most of Prompt Processing and not just the unit nominally being tested.

Solution
--------

Based on a dependency analysis of the code, [DM-47742](https://rubinobs.atlassian.net/browse/DM-47742) proposed breaking up `MiddlewareInterface` according to four fundamental responsibilities: local repo management, sending pipeline outputs, ingestion and image metadata, and pipeline management.
A new class dedicated to one (and only one!) of each of these responsibilities would take over natural groupings of fields and methods from the old `MiddlewareInterface`.

Although [Suryanarayana2015] recommend dealing with excessive technical debt by dedicating the entire team to refactoring and pausing all new development, this kind of focused sprint is difficult to sustain without an experienced architect.
Instead, DM-47742 favors an incremental approach where specific responsibilities would be removed from `MiddlewareInterface` one by one.
While this has the disadvantage of needing to account for concurrent development, it also gives room to re-examine the natural breakdown of responsibilities, and lets us use small-scale refactoring techniques like method extraction rather than demanding a monolithic redesign.

The project does not specifically address the hub problem, although breaking up the code into separate files immediately breaks up the dependency list.
The hope is that four smaller, less hub-like modules will be easier to decouple in later work.
Meanwhile, the original design goal of isolating all Middleware dependencies from the rest of the code still applies, just to the cluster of new modules instead of to `middleware_interface` alone.

Results so far
--------------

As of May 2026, the only part of this project that has been completed is extracting the local repo management functionality on [DM-47743](https://rubinobs.atlassian.net/browse/DM-47743).
Even this apparently well-defined task suffered from significant delays, partly through spin-off tasks and partly from the difficulty of visualizing exactly how to break up the old code.

The results showed the value of doing the refactoring incrementally.
The original plan in DM-47742 had treated all of preload as a single operation.
Only after work started on `LocalRepo` did it become clear that there is a natural division -- `MiddlewareInterface` (and its visit- and pipeline-dependent code) should be responsible for deciding which datasets are needed, while `LocalRepo` can handle the transfer, caching, and registration of an "order" of datasets from `MiddlewareInterface` without knowing how that set was generated.
The result is a cleaner separation of responsibilities than in the original high-level proposal.
On the other hand, the factoring out of `LocalRepo` left collection management awkwardly split between multiple methods, and this is something that will need to be revisited.

References
----------

[Suryanarayana2015] Suryanarayana, Girish; Samarthyam, Ganesh; and Sharma, Tushar. _Refactoring for Software Design Smells: Managing Technical Debt_. Morgan Kaufmann, 2015.
