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


def get_devcenter_project_pool_list(credentials: Credentials, subscription_id: str) -> List[Dict]:
    try:
        client = get_client(credentials, subscription_id)
        dcpp_list = list(map(lambda x: x.as_dict(), client.pools.list_by_project()))

        for dcpp in dcpp_list:
            x = dcpp['id'].split('/')
            dcpp['resource_group'] = x[x.index('resourceGroups') + 1]
            dcpp['resource_group_id'] = f'/subscriptions/{subscription_id}/resourceGroups/{dcpp['resource_group']}'

        return dcpp_list

    except HttpResponseError as e:
        logger.warning(f"Error while retrieving Dev Center Projects - {e}")
        return []

def load_dcpps(neo4j_session: neo4j.Session, subscription_id: str, dcpp_list: List[Dict], update_tag: int) -> None:
    ingest_dcpp = """
    UNWIND $dcpps AS dcpp
    MERGE (d:AzureDevCenterProjectPool{id: dcpp.id})
    ON CREATE SET 
        d.firstseen = timestamp(),
        d.type = dcpp.type, 
        d.location = dcpp.location,
        d.resourcegroup = dcpp.resource_group,
        d.resource_group_id = dcpp.resource_group_id
    SET 
        d.lastupdated = $update_tag, 
        d.name = dcpp.name,
        d.description = dcpp.description,
        d.provisioning_state = dcpp.provisioning_state,
        d.dev_center_id = dcpp.dev_center_id
    WITH d
        MATCH (dc:AzureDevCenterProject{id: d.project_id})
            MERGE (d)-[dcr:PROJECT]->(dc)
            ON CREATE SET dcr.firstseen = timestamp()
            SET dcr.lastupdated = $update_tag
    """

    for dcpp in dcpp_list:
        logger.warning(f"dcpp: {dcpp}")

    # for dcpp in dcpp_list:
    #     neo4j_session.run(
    #         ingest_dcpp,
    #         dcpps=dcpp,
    #         AZURE_DEVCENTER=dcpp['dev_center_id'],
    #         update_tag=update_tag,
    # )


#def cleanup_resource_group(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
#    run_cleanup_job('azure_resource_group_cleanup.json', neo4j_session, common_job_parameters)


def sync_dev_center_project_pool(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    dcpp_list = get_devcenter_project_pool_list(credentials, subscription_id)
    load_dcpps(neo4j_session, subscription_id, dcpp_list, update_tag)
    #cleanup_resource_group(neo4j_session, common_job_parameters)


def sync(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Dev Centers for subscription '%s'.", subscription_id)

    sync_dev_center_project_pool(neo4j_session, credentials, subscription_id, update_tag, common_job_parameters)
