MATCH (f: PERSON)-[workAt:WORKAT]->(company:ORGANISATION)-[:ISLOCATEDIN]->(country:PLACE)
Return 
    CASE
        WHEN company is null Then null
        ELSE [company.name, workAt.workFrom, country.name]
    END as companies