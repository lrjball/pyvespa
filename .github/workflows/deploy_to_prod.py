# Command line script to deploy Vespa applications to Vespa Cloud
# Usage: vespa-deploy.py --tenant --application --api-key --application-root --max-wait 3600 --source-url

import argparse

from vespa.deployment import VespaCloud
import os


def deploy_prod(
    tenant, application, api_key, application_root, max_wait=3600, source_url=None
):
    key = os.getenv("VESPA_TEAM_API_KEY").replace(r"\n", "\n")
    print("Key == api_key")
    print(key == api_key)
    vespa_cloud = VespaCloud(
        tenant=tenant,
        application=application,
        key_content=api_key,
        application_root=application_root,
    )
    build_no = vespa_cloud.deploy_to_prod(application_root, source_url=source_url)
    vespa_cloud.wait_for_prod_deployment(build_no, max_wait=max_wait)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--tenant", required=True, help="Vespa Cloud tenant")
    args.add_argument("--application", required=True, help="Vespa Cloud application")
    args.add_argument(
        "--api-key", required=True, help="Vespa Cloud Control-plane API key"
    )
    args.add_argument(
        "--application-root", required=True, help="Path to the Vespa application root"
    )
    args.add_argument(
        "--max-wait", type=int, default=3600, help="Max wait time in seconds"
    )
    args.add_argument(
        "--source-url", help="Source URL (git commit URL) for the deployment"
    )

    args = args.parse_args()
    deploy_prod(args.tenant, args.application, args.api_key, args.application_root)
