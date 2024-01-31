# Temporal Batch

An easy-to-use library for reliably and scalably performing many like operations based on [Temporal](https://temporal.io) workflows and activities.
It supports
* controllably parallel execution, allowing you to process quickly while limiting how heavily you 
  beat down your downstream dependencies
* pagination.  When you query a database for some results plus the next page token, you
  simply signal the workflow with the next page token before you start processing the results.
* v1 reliably processes up to approximately 1000 pages of data.  We know how to support higher scale but haven't built it yet; let us know.

All you have to build is an ![Activity](https://docs.temporal.io/activities) to process manageable chunks.

# Quick Start

To develop anything using temporal on your machine, first get a [local Temporal server running](https://docs.temporal.io/application-development/foundations#run-a-development-cluster)

Then look at the language-specific quickstart guides:

* [Python](./python/README.md)


# Architecture:
![Alt text](architecture_diagram.png "Architecture Diagram") 



