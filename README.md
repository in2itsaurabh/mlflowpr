import dagshub
dagshub.init(repo_owner='saurabhin2it', repo_name='mlflowpr', mlflow=True)

import mlflow
with mlflow.start_run():
  mlflow.log_param('parameter name', 'value')
  mlflow.log_metric('metric name', 1)