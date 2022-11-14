# Databricks notebook source
# MAGIC %md
# MAGIC ### 创建Webhooks
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC <img src="https://github.com/RafiKurlansik/laughing-garbanzo/blob/main/webhooks2.png?raw=true" width = 600>

# COMMAND ----------

import mlflow
from mlflow.utils.rest_utils import http_request
import json

def client():
    return mlflow.tracking.client.MlflowClient()

host_creds = client()._tracking_client.store.get_host_creds()
host = host_creds.host
token = host_creds.token

def mlflow_call_endpoint(endpoint, method, body='{}'):
    if method == 'GET':
        response = http_request(
            host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
    else:
        response = http_request(
            host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
    return response.json()

dbutils.widgets.text("model_name", "")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transition Request Created
# MAGIC 
# MAGIC These fire whenever a transition request is created for a model.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 触发作业

# COMMAND ----------

# TODO, https://docs.databricks.com/applications/mlflow/model-registry-webhooks.html
# Register with the right stage
# TRANSITION_REQUEST_TO_STAGING_CREATED
# TRANSITION_REQUEST_TO_PRODUCTION_CREATED
# MODEL_VERSION_TRANSITIONED_TO_STAGING
# MODEL_VERSION_TRANSITIONED_TO_PRODUCTION

# COMMAND ----------

model_name = dbutils.widgets.get("model_name")

# COMMAND ----------

# 我们将为注册表中的哪个模型创建webhook?
import json

host = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# 改变它!在虚拟笔记本上创建一个工作
job_id = "335219641933529"

# "events": ["MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"],

trigger_job = json.dumps({
  "model_name": model_name,
  "events": ["MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"],
  "description": "Trigger the dummy job when a model is moved to production.",
  "status": "ACTIVE",
  "job_spec": {
    "job_id": job_id,    # This is our dummy notebook
    "workspace_url": host,
    "access_token": token
  }
})

# COMMAND ----------

mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_job)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notifications
# MAGIC 
# MAGIC Webhooks can be used to send emails, Slack messages, and more.  In this case we use Slack.  We also use `dbutils.secrets` to not expose any tokens, but the URL looks more or less like this:
# MAGIC 
# MAGIC `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`
# MAGIC 
# MAGIC You can read more about Slack webhooks [here](https://api.slack.com/messaging/webhooks#create_a_webhook).

# COMMAND ----------

import urllib 
import json 

try:
  # TODO, set up a slack webhook
  slack_webhook = dbutils.secrets.get("rk_webhooks", "slack") # You have to set up your own webhook!
except:
  slack_webhook = None
  
#     "events": ["TRANSITION_REQUEST_CREATED"],  

if slack_webhook:
  # consider REGISTERED_MODEL_CREATED to run tests and autoamtic deployments to stages 
  trigger_slack = json.dumps({
    "model_name": churn_model_name,
    "events": ["MODEL_VERSION_TRANSITIONED_TO_STAGING"],
    "description": "Notify the MLOps team that a model has moved from None to Staging.",
    "status": "ACTIVE",
    "http_url_spec": {
      "url": slack_webhook
    }
  })
  mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_slack)
else:
  print("We don't have Slack hook, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notifications

# COMMAND ----------

import urllib 
import json 

#     "events": ["MODEL_VERSION_TRANSITIONED_STAGE"],

if slack_webhook:
  trigger_slack = json.dumps({
    "model_name": churn_model_name,
    "events": ["MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"],
    "description": "Notify the MLOps team that a model has moved from Staging to Production.",
    "http_url_spec": {
      "url": slack_webhook
    }
  })
  mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_slack)
else:
  print("We don't have Slack hook, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Webhooks

# COMMAND ----------

# MAGIC %md
# MAGIC ##### List 

# COMMAND ----------

list_model_webhooks = json.dumps({"model_name": model_name})
mlflow_call_endpoint("registry-webhooks/list", method = "GET", body = list_model_webhooks)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delete

# COMMAND ----------

# Remove a webhook
mlflow_call_endpoint("registry-webhooks/delete",
                     method="DELETE",
                     body = json.dumps({'id': 'e6943655e1444dbb8ff59fbe964c2644'}))

# COMMAND ----------

