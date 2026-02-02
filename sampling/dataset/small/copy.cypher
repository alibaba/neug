-- Data import commands for ldbc_small dataset
-- Run this file after creating the schema to import data

-- Import vertex data
COPY person FROM "person.csv" (header=true, delimiter="|");
COPY comment FROM "comment.csv" (header=true, delimiter="|");
COPY post FROM "post.csv" (header=true, delimiter="|");

-- Import edge data
COPY person_knows_person FROM "person_knows_person.csv" (
    from="person", to="person", header=true, delimiter="|"
);
COPY comment_hasCreator_person FROM "comment_hasCreator_person.csv" (
    from="comment", to="person", header=true, delimiter="|"
);
COPY post_hasCreator_person FROM "post_hasCreator_person.csv" (
    from="post", to="person", header=true, delimiter="|"
);
