# temporal-batch
A easy-to-use library for reliably and scalably performing many operations.
Based on Temporal workflows and activities.
It supports
* controllably parallel execution, allowing you to process quickly while limiting how heavily you 
  beat down your downstream dependencies
* pagination.  When you query a database for some results plus the next page token, you
  simply signal the workflow with the next page token before you start processing the results.
* v1 reliably supports up to approximately 4000 minutes of total execution time.  Please let me know if you want higher scale.

All you have to build is an ![Activity](https://docs.temporal.io/activities) to process manageaable chunks
See ![Alt text](architecture_diagram.png "Architecture Diagram") 

Currently supported languages
* Python (depends on https://github.com/temporalio/sdk-python)

