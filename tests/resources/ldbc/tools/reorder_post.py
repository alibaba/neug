import duckdb
import sys

input_dir = sys.argv[1]
output_file = sys.argv[2]

print(input_dir)

con = duckdb.connect(database = ':memory:', read_only = False)
con.execute("""
CREATE TABLE post as SELECT * FROM read_csv_auto('%s/post_0_0.csv', delim='|', columns={'id': 'BIGINT', 'imageFile': 'VARCHAR', 'creationDate': 'VARCHAR', locationIP: 'VARCHAR', browserUsed: 'VARCHAR', 'language': 'VARCHAR', content: 'VARCHAR', length: 'INTEGER'})
        """ % (input_dir,))
con.execute("""
CREATE TABLE creator as SELECT * FROM read_csv_auto('%s/post_hasCreator_person_0_0.csv', delim='|')
        """ % (input_dir,))
print(con.execute("DESC creator").fetchall())
con.execute("""
CREATE TABLE reorder as SELECT 
    creator."Person.id",
    post.id,
    post.imageFile,
    post.creationDate,
    post.locationIP,
    post.browserUsed,
    post.language,
    post.content,
    post.length
FROM creator
LEFT JOIN
    post 
ON creator."Post.id" = post.id
        """)

result = con.execute("""
COPY
(
SELECT 
id, imageFile,
creationDate,
locationIP, browserUsed, language, content, length from reorder ORDER BY "Person.id"
)
TO '%s' (FORMAT CSV, DELIMITER '|')
        """ % (output_file,))

# with open(output_file, 'w') as f:
#     f.write('|'.join([str(i[0]) for i in result.description]) + '\n')
#     delimiter = '|'
#     for row in result.fetchall():
#         f.write(delimiter.join(map()) + '\n')
#         # f.write(delimiter.join(map(str, row)) + '\n')
