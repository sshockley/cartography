import logging
from typing import Dict
from typing import List

import neo4j
from azure.core.exceptions import HttpResponseError
from azure.mgmt.resource import ResourceManagementClient

from .util.credentials import Credentials
from cartography.util import run_cleanup_job
from cartography.util import timeit

import json

logger = logging.getLogger(__name__)

def get_client(credentials: Credentials, subscription_id: str) -> ResourceManagementClient:
    client = ResourceManagementClient(credentials, subscription_id)
    return client

def get_rg_list(credentials: Credentials, subscription_id: str) -> List[Dict]:
    try:
        client = get_client(credentials, subscription_id)

        #rg_list = client.resource_groups.list()
        rg_list = list(map(lambda x: x.as_dict(), client.resource_groups.list()))

        return rg_list

    except HttpResponseError as e:
        logger.warning(f"Error while retrieving resource groups - {e}")
        return []


def load_rgs(neo4j_session: neo4j.Session, subscription_id: str, rg_list: List[Dict], update_tag: int) -> None:
    ingest_rg = """
    UNWIND $rg_list AS rg
    MERGE (r:AzureResourceGroup{id: rg.id})
    ON CREATE SET 
        r.firstseen = timestamp(),
        r.name = rg.name,
        r.location = rg.location
    SET
        r.lastupdated = $update_tag, 
        r.name = rg.name,
        r.location = rg.location
    WITH r
        MATCH (owner:AzureSubscription{id: $SUBSCRIPTION_ID})
            MERGE (owner)-[s:RESOURCE]->(r)
            ON CREATE SET s.firstseen = timestamp()
            SET s.lastupdated = $update_tag
    """

    #for rg in rg_list:
    #    logger.warning(f"rg: {rg}")

    neo4j_session.run(
        ingest_rg,
        rg_list=rg_list,
        SUBSCRIPTION_ID=subscription_id,
        update_tag=update_tag,
    )


def cleanup_resource_group(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
    run_cleanup_job('azure_resource_group_cleanup.json', neo4j_session, common_job_parameters)


def sync_resource_group(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    rg_list = get_rg_list(credentials, subscription_id)
    load_rgs(neo4j_session, subscription_id, rg_list, update_tag)
    cleanup_resource_group(neo4j_session, common_job_parameters)


def sync(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Resource Groups for subscription '%s'.", subscription_id)

    sync_resource_group(neo4j_session, credentials, subscription_id, update_tag, common_job_parameters)
