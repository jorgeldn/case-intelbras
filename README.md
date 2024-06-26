# Intebras Case

Case de pipeline de dados no GCP Dataflow.

## Etapas:

1. Criar bucket e carregar dados na camada RAW-> **create_bucket.py**
2. Ler e transformar dados da camada *RAW* e salvar em *Trusted* -> **job-dataflow-trusted**
3. Ler e agrupar dados da camada *Trusted* e salvar em *Refined* -> **job-dataflow-refined**
4. Ler dados de *Refined* e carregar no BigQuery -> **job-dataflow-to-bq**
