This directory provides useful set of examples who to adopt
LifeCycle agent to specific application.

### DAS

DAS, CMS Data Aggregation Service, collects and aggregates information
from various CMS data-services.
This directory contains examples how to adopt LifeCycle agent for DAS needs.
The example-DAS.conf file is LifeCycle configuration for DAS workflow. It has
four steps:

- das-spawn-workflows.py, prepares DAS workflows
- das-fake-make-request.py, make DAS requests
- das-fake-poll-request.py, poll DAS requests
- das-whats-next.py, collects results and print them to stdout

All modules uses utils/options sub-module and implement actions specified in
example-DAS.conf configuration file.
