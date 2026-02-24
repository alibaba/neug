import duckdb
import sys

input_dir = sys.argv[1]
output_file = sys.argv[2]

print(input_dir)

con = duckdb.connect(database = ':memory:', read_only = False)
con.execute("""
CREATE TABLE comment as SELECT * FROM read_csv_auto('%s/comment_0_0.csv', delim='|', columns={'id': 'BIGINT', 'creationDate': 'VARCHAR', locationIP: 'VARCHAR', browserUsed: 'VARCHAR', content: 'VARCHAR', length: 'INTEGER'})
        """ % (input_dir,))
print(con.execute("DESC comment").fetchall())
con.execute("""
CREATE TABLE creator as SELECT * FROM read_csv_auto('%s/comment_hasCreator_person_0_0.csv', delim='|')
        """ % (input_dir,))
print(con.execute("DESC creator").fetchall())
con.execute("""
CREATE TABLE reorder as SELECT 
    creator."Person.id",
    comment.id,
    comment.creationDate,
    comment.locationIP,
    comment.browserUsed,
    comment.content,
    comment.length
FROM creator
LEFT JOIN
    comment
ON creator."Comment.id" = comment.id
        """)

result = con.execute("""
SELECT 
id, 
creationDate,
locationIP, browserUsed, content, length from reorder ORDER BY "Person.id"
        """)

with open(output_file, 'w') as f:
    f.write('|'.join([str(i[0]) for i in result.description]) + '\n')
    delimiter = '|'
    for row in result.fetchall():
        f.write(delimiter.join(map(str, row)) + '\n')