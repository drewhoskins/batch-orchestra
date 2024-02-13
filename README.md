# Batch Orchestra

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
## What problems does batch-orchestra help me solve?
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

## Why should I trust this framework?
* It is based on Temporal workflows.  Temporal is known for its feature richness, along with its robustness and distributed systems heavy lifting.
* It's based on a design built within Stripe where it was both robust and popular.

## What types of people will use this framework?
Early, users who already use Temporal or are interested in setting up Temporal for their project will be the most interested.  
It's TBD whether/to what extent this is better than other frameworks outside the Temporal orbit.

## What are the caveats?
This framework does not guarantee that items in your data set are processed in a certain order.
Think about the operation you want to apply to each item in your data set.
* If it's "cheap" (less than a network call), this framework will be overkill--for example, if your input and output are both columnar database tables.
* If it's complex, this framework can work, but it does not help you track the state transitions or dependencies within the operation, so code carefully.
* If it's non-idempotent, as with any framework, you will want to make it idempotent or turn off retries if you want to be safe.
* Currently scales to roughly 500 pages.  Higher scale support is coming soon--please ask for it.  (see [Very Long Running Workflows](https://temporal.io/blog/very-long-running-workflows))


# Approach:
Batch Orchestra uses "pipelined pagination."

## Prior art: parallel pagination
Normally, when you paginate through a data set in parallel, you 
1. determine the page boundaries
2. "fan out" different pages to different workers to execute in parallel
3. Record each time a page completes.
Step 1 is problematic.  How do you know what the page boundaries should be?  If you have a static file with numbered lines, you can easily calculate the pages up front by passing line numbers.  But in the real world, you often need to paginate based on a primary key or timestamp.  You need to either do a full scan of your data set, or make guesses as to how to divide the data, such as processing data in 5 minute time increments and hoping that the data are evenly distributed among time slices.

## Prior art: serial pagination
Normally, when you paginate through a data set serially, in a loop, you 
1. fetch the contents of a page, 
2. process each item
3. calculate the cursor for the next page
4. Record completion through the cursor so that if your process fails, it can resume from where it left off.
This approach is simple but allows no parallelism.

## Pipelined pagination
With Batch Orchestra, you code like you are doing serial pagination, but you invert steps 2 and 3.  The cursor is handed off to the orchestrator to execute in parallel while you process each item.
This gives you the simplicity of serial pagination along with parallelism, fair page sizes, and requires only one scan through the data set.
(P.S. Please let me know if you've seen other frameworks adopt this approach or know of an existing term for it.)

It looks like this:
![Alt text](architecture_diagram.png "Architecture Diagram") 

