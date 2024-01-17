
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

        # for item in devcenter_project_pools:
        #     logger.warning(f"item: {item}")

        return devcenter_project_pools

    except HttpResponseError as e:
        logger.warning(f"Error while retrieving Dev Center Project Pools - {e}")
        return []

def load_devcenter_project_pools(
        neo4j_session: neo4j.Session, devcenter_project_pools: List[Dict], update_tag: int,
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
        d.dev_center_id = dcpp.dev_center_id
    WITH d
        MATCH (dc:AzureDevCenterProject{id: d.project_id})
            MERGE (d)-[dcr:PROJECT]->(dc)
            ON CREATE SET dcr.firstseen = timestamp()
            SET dcr.lastupdated = $update_tag
    """

    # for dcpp in devcenter_project_pools:
    #     logger.warning(f"dcpp: {dcpp}")

    #for dcpp in devcenter_project_pools:
    neo4j_session.run(
        ingest_dcpp,
        dcpps=devcenter_project_pools,
        update_tag=update_tag
    )
