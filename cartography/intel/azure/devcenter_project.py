import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Generator
#from typing import Tuple

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
    #logger.warning(f"credentials: {credentials}")
    #logger.warning(f"client: {client}")
    return client


def get_devcenter_project_list(credentials: Credentials, subscription_id: str) -> List[Dict]:
    try:
        client = get_client(credentials, subscription_id)
        dcp_list = list(map(lambda x: x.as_dict(), client.projects.list_by_subscription()))

        for dcp in dcp_list:
            x = dcp['id'].split('/')
            dcp['resource_group'] = x[x.index('resourceGroups') + 1]
            dcp['resource_group_id'] = f'/subscriptions/{subscription_id}/resourceGroups/{dcp['resource_group']}'

        return dcp_list

    except HttpResponseError as e:
        logger.warning(f"Error while retrieving Dev Center Projects - {e}")
        return []


def load_devcenter_project_data(
    neo4j_session: neo4j.Session, 
    subscription_id: str, 
    dcp_list: List[Dict], 
    update_tag: int
) -> None:
    ingest_dcp = """
    UNWIND $dcps AS dcp
    MERGE (d:AzureDevCenterProject{id: dcp.id})
    ON CREATE SET 
        d.firstseen = timestamp(),
        d.type = dcp.type, 
        d.location = dcp.location,
        d.resourcegroup = dcp.resource_group,
        d.resource_group_id = dcp.resource_group_id
    SET 
        d.lastupdated = $update_tag, 
        d.name = dcp.name,
        d.description = dcp.description,
        d.provisioning_state = dcp.provisioning_state,
        d.dev_center_id = dcp.dev_center_id
    WITH d
        MATCH (dc:AzureDevCenter{id: d.dev_center_id})
            MERGE (d)-[dcr:PROJECT]->(dc)
            ON CREATE SET dcr.firstseen = timestamp()
            SET dcr.lastupdated = $update_tag
    """

    # for dcp in dcp_list:
    #     logger.warning(f"dcp: {dcp}")

    for dcp in dcp_list:
        neo4j_session.run(
            ingest_dcp,
            dcps=dcp,
            AZURE_DEVCENTER=dcp['dev_center_id'],
            update_tag=update_tag,
    )


#def cleanup_resource_group(neo4j_session: neo4j.Session, common_job_parameters: Dict) -> None:
#    run_cleanup_job('azure_resource_group_cleanup.json', neo4j_session, common_job_parameters)


# def sync_dev_center_project(
#     neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
#     common_job_parameters: Dict,
# ) -> None:
#     dcp_list = get_devcenter_project_list(credentials, subscription_id)
#     load_devcenter_project_data(neo4j_session, subscription_id, dcp_list, update_tag)
#     #cleanup_resource_group(neo4j_session, common_job_parameters)


# def get_dev_center_project_details(
#         credentials: Credentials, 
#         subscription_id: str, 
#         devcenter_project_list: List[Dict],
# ) -> List[Dict]:
# #) -> Generator[Any, Any, Any]:
#     """
#     Iterate over each project to get its children.
#     """
#     for dcp in devcenter_project_list:
#         devcenter_project_pools = list(map(lambda x: x.as_dict(), get_devcenter_project_pools(credentials, subscription_id, dcp)))
#     # devcenter_project_pools = []
#     # for dcp in devcenter_project_list:
#     #     devcenter_project_pools.append(get_devcenter_project_pools(credentials, subscription_id, dcp))

#     return devcenter_project_pools

def get_devcenter_project_pools(
    credentials: Credentials, 
    subscription_id: str, 
    devcenter_project: str
) -> List[Dict]:
    try:
        client = get_client(credentials, subscription_id)
        #logger.warning(f"devcenter_project: {devcenter_project}")
        devcenter_project_pools = list(map(lambda x: x.as_dict(), 
            client.pools.list_by_project(resource_group_name=devcenter_project['resource_group'], project_name=devcenter_project['name'])))

        for dcpp in devcenter_project_pools:
            x = dcpp['id'].split('/')
            dcpp['resource_group'] = x[x.index('resourceGroups') + 1]
            dcpp['resource_group_id'] = f'/subscriptions/{subscription_id}/resourceGroups/{dcpp['resource_group']}'
            dcpp['project_id'] = f'{devcenter_project['id']}'

        # for item in devcenter_project_pools:
        #     logger.warning(f"item: {item}")

        return devcenter_project_pools

    except HttpResponseError as e:
        logger.warning(f"Error while retrieving Dev Center Project Pools - {e}")
        return []


def load_devcenter_project_pools(
        neo4j_session: neo4j.Session, 
        devcenter_project_pools: List[Dict], 
        update_tag: int,
) -> None:
    """
    Ingest the devcenter project pool details into neo4j.
    """
    ingest_dcpp = """
    UNWIND $dcpps AS dcpp
    MERGE (d:AzureDevCenterProjectPool{id: dcpp.id})
    ON CREATE SET 
        d.firstseen = timestamp()
    SET 
        d.type = dcpp.type, 
        d.location = dcpp.location,
        d.resourcegroup = dcpp.resource_group,
        d.resource_group_id = dcpp.resource_group_id,
        d.lastupdated = $update_tag, 
        d.name = dcpp.name,
        d.description = dcpp.description,
        d.provisioning_state = dcpp.provisioning_state,
        d.dev_center_id = dcpp.dev_center_id,
        d.project_id = dcpp.project_id
    WITH d
        MATCH (dc:AzureDevCenterProject{id: d.project_id})
            MERGE (d)-[dcr:PROJECT]->(dc)
            ON CREATE SET dcr.firstseen = timestamp()
            SET dcr.lastupdated = $update_tag
    """

    for dcpp in devcenter_project_pools:
        logger.warning(f"dcpp: {dcpp}")

    neo4j_session.run(
        ingest_dcpp,
        dcpps=devcenter_project_pools,
        update_tag=update_tag
    )


def sync(
    neo4j_session: neo4j.Session, credentials: Credentials, subscription_id: str, update_tag: int,
    common_job_parameters: Dict,
) -> None:
    logger.info("Syncing Dev Centers for subscription '%s'.", subscription_id)

    #sync_dev_center_project(neo4j_session, credentials, subscription_id, update_tag, common_job_parameters)
    devcenter_project_list = get_devcenter_project_list(credentials, subscription_id)
    load_devcenter_project_data(neo4j_session, subscription_id, devcenter_project_list, update_tag)
    #cleanup_resource_group(neo4j_session, common_job_parameters)

    # for dcp in dcp_list:
    #     logger.warning(f"dcp: {dcp}")

    for devcenter_project in devcenter_project_list:
        devcenter_project_pools = get_devcenter_project_pools(
            credentials, 
            subscription_id, 
            devcenter_project
        )
        load_devcenter_project_pools(
            neo4j_session, 
            devcenter_project_pools, 
            update_tag
        )




