# synthetic-ti-reports
Using ML NLP models to generate synthetic Threat Intelligence Reports

This is a project/jupyter notebook/prefect job to parse Threat Intelligence reports gathered by 
the [Threat Intel Collector](https://github.com/g-clef/ThreatIntelCollector), and use them as a domain-specific 
set of documents to train a machine-learning NLP model. The end goal is to create an NLP model that can 
generate vaguely-realistic sounding (or better yet, hilariously wrong) synthetic Threat Intelligence
reports.

## Plan

The jupyter notebook will go into more detail, but at a high level, this will require a few moving parts:
 1. a Prefect job to parse the report PDFs, and extract the text of the reports
 2. an NLP model
 3. a system with a reasonable GPU (or a cloud GPU that can access the data generated in step 1)

The prefect job should run inside the same k8s cluster that's running the
[malwarETL](https://github.com/g-clef/malwarETL-k8s) system, since in my case, that's where the Threat Intel 
Collector is running, and where I have prefect jobs run with access to the various things collected in the
cluster.
