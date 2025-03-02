import json
import time
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

def load_config(config_path):
    """Load config values from a JSON file."""
    with open(config_path, 'r') as file:
        return json.load(file)

def trigger_pipeline(config):
    """
    Authenticate using the service principal created in Entra and trigger the Data Factory pipeline.
    Then poll the pipeline run status until it completes.
    """
    # Extract config values
    tenant_id = config["tenant_id"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]
    subscription_id = config["subscription_id"]
    resource_group_name = config["resource_group_name"]
    factory_name = config["factory_name"]
    pipeline_name = config["pipeline_name"]

    # Auth using the service principal credentials
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    # Create the Data Factory management client
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Trigger the pipeline run
    run_response = adf_client.pipelines.create_run(
        resource_group_name,
        factory_name,
        pipeline_name
    )
    print(f"Pipeline run initiated. Run ID: {run_response.run_id}")

    # Poll for pipeline run status until completion
    pipeline_run_id = run_response.run_id
    while True:
        pipeline_run = adf_client.pipeline_runs.get(resource_group_name, factory_name, pipeline_run_id)
        print(f"Current pipeline status: {pipeline_run.status}")
        if pipeline_run.status in ["Succeeded", "Failed", "Cancelled"]:
            break
        time.sleep(2)
    
    print("Pipeline run completed.")

def main():
    """Main method: load config and trigger the pipeline."""
    config = load_config("config.json")
    trigger_pipeline(config)

if __name__ == "__main__":
    main()