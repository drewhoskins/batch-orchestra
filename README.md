# Temporal Batch

An easy-to-use library for reliably and scalably performing many like operations based on [Temporal](https://temporal.io) workflows and activities.
It supports
* controllably parallel execution across many workers, allowing you to process quickly while limiting how heavily you 
  beat down your downstream dependencies
* pagination.  When you query a database for some results plus the next page token, you
  simply signal the workflow with the next page token before you start processing the results.


All you have to build is an ![Activity](https://docs.temporal.io/activities) to process manageable chunks.

# Quick Start

To develop anything using temporal on your machine, first get a [local Temporal server running](https://docs.temporal.io/application-development/foundations#run-a-development-cluster)

Then look at the language-specific quickstart guides:

* [Python](./python/README.md)

# FAQ
## Why should I trust this framework?
* It is a relatively thin veneer on top of Temporal workflows.  Temporal is known for its feature richness, along with scaling and distributed systems heavy lifting.
* It was originally built within Stripe where it was both scalable and popular.

## What problems does it help me solve?
It's designed to iterate through a data set (CSV, database) and parallel process that data.  
For example, data migrations or calling an API for each item in a data set.

It helps solve many problems that tend to come up for these cases.
* Can run on tens or hundreds of processes in parallel.
* Eliminates unevenly-sized pages without needing to first traverse the database to find the page boundaries.
* Controllable parallelism to limit impact on downstream systems, including [coming soon] pauses and gradual rampups.
* Feature-rich retries support.
* Built-in progress tracking.
* Debuggable, via Temporal's UI.
* Rich failure handling:
** Code deploys can fix your running batch without need for other remediations.
** Built-in failure and stuck pages tracking.
** Avoids stuck pages blocking progress.
** [coming soon] Allows you to avoid head-of-line blocking.

Check out the ![samples](./python/samples/README.md) to get a better idea.

## What types of people will use this framework?
Early, users who already use Temporal or are interested in setting up Temporal for their project will be the most interested.  
It's TBD whether/to what extent this is better than other frameworks outside the Temporal orbit.

## What are the caveats?
Think about the operation you want to apply to each item in your data set.
* If it's "cheap" (less than a network call), this framework will be overkill--for example, if your input and output are both columnar database tables.
* If it's complex, this framework can work, but it does not help you track the state transitions or dependencies within the operation, so code carefully.
* If it's non-idempotent, as with any framework, you will want to make it idempotent or turn off retries if you want to be safe.
* Currently scales to roughly 500 pages.  Higher scale support is coming soon--please ask for it.  (see [Very Long Running Workflows](https://temporal.io/blog/very-long-running-workflows))

# Architecture:
![Alt text](architecture_diagram.png "Architecture Diagram") 



