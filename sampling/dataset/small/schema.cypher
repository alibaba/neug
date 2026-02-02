-- Schema definition for ldbc_small dataset
-- Run this file to create the graph schema in NeuG

-- Create person node table
CREATE NODE TABLE person(
    id INT32 PRIMARY KEY,
    firstName STRING,
    lastName STRING,
    gender STRING,
    birthday STRING,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    email STRING
);

-- Create comment node table
CREATE NODE TABLE comment(
    id INT32 PRIMARY KEY,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    content STRING,
    length INT32
);

-- Create post node table
CREATE NODE TABLE post(
    id INT32 PRIMARY KEY,
    imageFile STRING,
    creationDate STRING,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    content STRING,
    length INT32
);

-- Create relationship tables
CREATE REL TABLE person_knows_person(
    FROM person TO person,
    creationDate STRING
);

CREATE REL TABLE comment_hasCreator_person(
    FROM comment TO person
);

CREATE REL TABLE post_hasCreator_person(
    FROM post TO person
);
