import logging
from typing import Dict
from typing import List

import neo4j
from azure.core.exceptions import HttpResponseError
from azure.mgmt.devcenter import DevCenterMgmtClient

from .util.credentials import Credentials
from cartography.util import run_cleanup_job
from cartography.util import timeit

logger = logging.getLogger(__name__)

def get_client(credentials: Credentials, subscription_id: str) -> DevCenterMgmtClient:
    logging.getLogger('azure.identity').setLevel(logging.WARNING)
    client = DevCenterMgmtClient(credentials, subscription_id)
    return client


def get_devcenter_list(credentials: Credentials, subscription_id: str) -> List[Dict]:
    try:
        client = get_client(credentials, subscription_id)
        dc_list = list(map(lambda x: x.as_dict(), client.dev_centers.list_by_subscription()))

        for dc in dc_list:
            x = dc['id'].split('/')
            dc['resource_group'] = x[x.index('resourceGroups') + 1]
            dc['resource_group_id'] = f'/subscriptions/{subscription_id}/resourceGroups/{dc['resource_group']}'

        return dc_list

    except HttpResponseError as e:
        logger.warning(f"Error while retrieving Dev Centers - {e}")
        return []

def load_dcs(neo4j_session: neo4j.Session, subscription_id: str, dc_list: List[Dict], update_tag: int) -> None:
    ingest_dc = """
    UNWIND $dcs AS dc
    MERGE (d:AzureDevCenter{id: dc.id})
    ON CREATE SET 
        d.firstseen = timestamp(),
        d.type = dc.type, 
        d.location = dc.location,
        d.resourcegroup = dc.resource_group,
        d.resource_group_id = dc.resource_group_id
    SET 
        d.lastupdated = $update_tag, 
        d.name = dc.name
    WITH d
        MATCH (owner:AzureResourceGroup{id: $AZURE_RESOURCE_GROUP})
            MERGE (owner)-[r:RESOURCE]->(d)
            ON CREATE SET r.firstseen = timestamp()
            SET r.lastupdated = $update_tag
    """

    #for dc in dc_list:
    #    logger.warning(f"dc: {dc}")

    for dc in dc_list:
        neo4j_session.run(
            ingest_dc,
            dcs=dc,
            AZURE_RESOURCE_GROUP=dc['resource_group_id'],
            update_tag=update_tag,
    )


    # neo4j_session.run(
    #     ingest_dc,
    #     dc_list=dc_list,
    #     SUBSCRIPTION_ID=subscription_id,
    #     update_tag=update_tag,
    # )

#def cleanup_resource_group(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
#    run_cleanup_job('azure_resource_group_cleanup.json', neo4j_session, common_job_parameters)


def sync_dev_center(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    dc_list = get_devcenter_list(credentials, subscription_id)
    load_dcs(neo4j_session, subscription_id, dc_list, update_tag)
    #cleanup_resource_group(neo4j_session, common_job_parameters)


def sync(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Dev Centers for subscription '%s'.", subscription_id)

    sync_dev_center(neo4j_session, credentials, subscription_id, update_tag, common_job_parameters)
