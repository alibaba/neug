// ============================================================
// LDBC SNB Schema DDL & Data Loading
// ============================================================

// -------------------- Vertex Tables --------------------

CREATE NODE TABLE PLACE (
    id INT64,
    name STRING,
    url STRING,
    type STRING,
    PRIMARY KEY (id)
);

CREATE NODE TABLE PERSON (
    id INT64,
    firstName STRING,
    lastName STRING,
    gender STRING,
    birthday DATE,
    creationDate TIMESTAMP,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    email STRING,
    PRIMARY KEY (id)
);

CREATE NODE TABLE COMMENT (
    id INT64,
    creationDate TIMESTAMP,
    locationIP STRING,
    browserUsed STRING,
    content STRING,
    length INT32,
    PRIMARY KEY (id)
);

CREATE NODE TABLE POST (
    id INT64,
    imageFile STRING,
    creationDate TIMESTAMP,
    locationIP STRING,
    browserUsed STRING,
    language STRING,
    content STRING,
    length INT32,
    PRIMARY KEY (id)
);

CREATE NODE TABLE FORUM (
    id INT64,
    title STRING,
    creationDate TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE NODE TABLE ORGANISATION (
    id INT64,
    type STRING,
    name STRING,
    url STRING,
    PRIMARY KEY (id)
);

CREATE NODE TABLE TAGCLASS (
    id INT64,
    name STRING,
    url STRING,
    PRIMARY KEY (id)
);

CREATE NODE TABLE TAG (
    id INT64,
    name STRING,
    url STRING,
    PRIMARY KEY (id)
);

// -------------------- Edge Tables --------------------

CREATE REL TABLE HASCREATOR (FROM COMMENT TO PERSON, creationDate TIMESTAMP);
CREATE REL TABLE HASCREATOR (FROM POST TO PERSON, creationDate TIMESTAMP);

CREATE REL TABLE HASTAG (FROM POST TO TAG);
CREATE REL TABLE HASTAG (FROM FORUM TO TAG);
CREATE REL TABLE HASTAG (FROM COMMENT TO TAG);

CREATE REL TABLE REPLYOF (FROM COMMENT TO COMMENT);
CREATE REL TABLE REPLYOF (FROM COMMENT TO POST);

CREATE REL TABLE CONTAINEROF (
    FROM FORUM TO POST
);

CREATE REL TABLE HASMEMBER (
    FROM FORUM TO PERSON,
    joinDate TIMESTAMP
);

CREATE REL TABLE HASMODERATOR (
    FROM FORUM TO PERSON
);

CREATE REL TABLE HASINTEREST (
    FROM PERSON TO TAG
);

CREATE REL TABLE ISLOCATEDIN (FROM COMMENT TO PLACE);
CREATE REL TABLE ISLOCATEDIN (FROM PERSON TO PLACE);
CREATE REL TABLE ISLOCATEDIN (FROM POST TO PLACE);
CREATE REL TABLE ISLOCATEDIN (FROM ORGANISATION TO PLACE);

CREATE REL TABLE KNOWS (
    FROM PERSON TO PERSON,
    creationDate TIMESTAMP
);

CREATE REL TABLE LIKES (FROM PERSON TO COMMENT, creationDate TIMESTAMP);
CREATE REL TABLE LIKES (FROM PERSON TO POST, creationDate TIMESTAMP);

CREATE REL TABLE WORKAT (
    FROM PERSON TO ORGANISATION,
    workFrom INT32
);

CREATE REL TABLE ISPARTOF (
    FROM PLACE TO PLACE
);

CREATE REL TABLE HASTYPE (
    FROM TAG TO TAGCLASS
);

CREATE REL TABLE ISSUBCLASSOF (
    FROM TAGCLASS TO TAGCLASS
);

CREATE REL TABLE STUDYAT (
    FROM PERSON TO ORGANISATION,
    classYear INT32
);

// -------------------- Load Vertex Data --------------------

COPY PLACE from "${DATA_PATH}/static/place_0_0.csv" (header=true, delimiter="|");
COPY PERSON FROM "${DATA_PATH}/dynamic/person_0_0.csv" (header=true, delimiter="|");
COPY COMMENT FROM "${DATA_PATH}/dynamic/comment_0_0_rod.csv" (header=true, delimiter="|");
COPY POST FROM "${DATA_PATH}/dynamic/post_0_0_rod.csv" (header=true, delimiter="|");
COPY FORUM FROM "${DATA_PATH}/dynamic/forum_0_0.csv" (header=true, delimiter="|");
COPY ORGANISATION FROM "${DATA_PATH}/static/organisation_0_0.csv" (header=true, delimiter="|");
COPY TAGCLASS FROM "${DATA_PATH}/static/tagclass_0_0.csv" (header=true, delimiter="|");
COPY TAG FROM "${DATA_PATH}/static/tag_0_0.csv" (header=true, delimiter="|");

// -------------------- Load Edge Data --------------------

COPY HASCREATOR FROM "${DATA_PATH}/dynamic/comment_hasCreator_person_0_0_creation_date.csv" (from="COMMENT", to="PERSON", header=true, delimiter="|");
COPY HASCREATOR FROM "${DATA_PATH}/dynamic/post_hasCreator_person_0_0_creation_date.csv" (from="POST", to="PERSON", header=true, delimiter="|");
COPY HASTAG FROM "${DATA_PATH}/dynamic/post_hasTag_tag_0_0.csv" (from="POST", to="TAG", header=true, delimiter="|");
COPY HASTAG FROM "${DATA_PATH}/dynamic/forum_hasTag_tag_0_0.csv" (from="FORUM", to="TAG", header=true, delimiter="|");
COPY HASTAG FROM "${DATA_PATH}/dynamic/comment_hasTag_tag_0_0.csv" (from="COMMENT", to="TAG", header=true, delimiter="|");
COPY REPLYOF FROM "${DATA_PATH}/dynamic/comment_replyOf_comment_0_0.csv" (from="COMMENT", to="COMMENT", header=true, delimiter="|");
COPY REPLYOF FROM "${DATA_PATH}/dynamic/comment_replyOf_post_0_0.csv" (from="COMMENT", to="POST", header=true, delimiter="|");
COPY CONTAINEROF FROM "${DATA_PATH}/dynamic/forum_containerOf_post_0_0.csv" (from="FORUM", to="POST", header=true, delimiter="|");
COPY HASMEMBER FROM "${DATA_PATH}/dynamic/forum_hasMember_person_0_0.csv" (from="FORUM", to="PERSON", header=true, delimiter="|");
COPY HASMODERATOR FROM "${DATA_PATH}/dynamic/forum_hasModerator_person_0_0.csv" (from="FORUM", to="PERSON", header=true, delimiter="|");
COPY HASINTEREST FROM "${DATA_PATH}/dynamic/person_hasInterest_tag_0_0.csv" (from="PERSON", to="TAG", header=true, delimiter="|");
COPY ISLOCATEDIN FROM "${DATA_PATH}/dynamic/comment_isLocatedIn_place_0_0.csv" (from="COMMENT", to="PLACE", header=true, delimiter="|");
COPY ISLOCATEDIN FROM "${DATA_PATH}/dynamic/person_isLocatedIn_place_0_0.csv" (from="PERSON", to="PLACE", header=true, delimiter="|");
COPY ISLOCATEDIN FROM "${DATA_PATH}/dynamic/post_isLocatedIn_place_0_0.csv" (from="POST", to="PLACE", header=true, delimiter="|");
COPY ISLOCATEDIN FROM "${DATA_PATH}/static/organisation_isLocatedIn_place_0_0.csv" (from="ORGANISATION", to="PLACE", header=true, delimiter="|");
COPY KNOWS FROM "${DATA_PATH}/dynamic/person_knows_person_0_0.csv" (from="PERSON", to="PERSON", header=true, delimiter="|");
COPY LIKES FROM "${DATA_PATH}/dynamic/person_likes_comment_0_0.csv" (from="PERSON", to="COMMENT", header=true, delimiter="|");
COPY LIKES FROM "${DATA_PATH}/dynamic/person_likes_post_0_0.csv" (from="PERSON", to="POST", header=true, delimiter="|");
COPY WORKAT FROM "${DATA_PATH}/dynamic/person_workAt_organisation_0_0.csv" (from="PERSON", to="ORGANISATION", header=true, delimiter="|");
COPY ISPARTOF FROM "${DATA_PATH}/static/place_isPartOf_place_0_0.csv" (from="PLACE", to="PLACE", header=true, delimiter="|");
COPY HASTYPE FROM "${DATA_PATH}/static/tag_hasType_tagclass_0_0.csv" (from="TAG", to="TAGCLASS", header=true, delimiter="|");
COPY ISSUBCLASSOF FROM "${DATA_PATH}/static/tagclass_isSubclassOf_tagclass_0_0.csv" (from="TAGCLASS", to="TAGCLASS", header=true, delimiter="|");
COPY STUDYAT FROM "${DATA_PATH}/dynamic/person_studyAt_organisation_0_0.csv" (from="PERSON", to="ORGANISATION", header=true, delimiter="|");
