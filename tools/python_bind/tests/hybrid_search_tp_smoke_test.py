#!/usr/bin/env python3

"""Smoke-test the vector and full-text features of hybrid_search."""

import socket
import tempfile
from pathlib import Path

from neug import Database
from neug import Session


def _unused_tcp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def main():
    with tempfile.TemporaryDirectory(prefix="neug_hybrid_search_") as tmp_dir:
        db = Database(
            db_path=str(Path(tmp_dir) / "database"),
            mode="w",
        )
        conn = db.connect()
        session = None
        try:
            # Use extensions produced by this build rather than downloading packages.
            conn.execute("LOAD vector_search;")
            conn.execute("LOAD fts;")
            conn.execute(
                "CREATE NODE TABLE Document("
                "id INT64 PRIMARY KEY, text STRING, embedding FLOAT[3]"
                ");"
            )
            conn.execute(
                "CREATE "
                "(:Document {id: 1, text: 'graph database engine', "
                "embedding: [1.0, 0.0, 0.0]}),"
                "(:Document {id: 2, text: 'vector search engine', "
                "embedding: [0.0, 1.0, 0.0]}),"
                "(:Document {id: 3, text: 'full text search with sqlite', "
                "embedding: [0.0, 0.0, 1.0]});"
            )

            conn.execute(
                "CREATE INDEX document_embedding_hnsw ON Document "
                "USING HNSW (embedding) "
                "WITH (metric = 'l2', m = 16, ef_construction = 100);"
            )
            conn.execute(
                "CREATE INDEX document_text_fts ON Document " "USING FTS (text);"
            )

            vector_rows = list(
                conn.execute(
                    "MATCH (n:Document) "
                    "RETURN n.id, "
                    "vector_distance_l2(n.embedding, [0.0, 0.9, 0.1]) AS score "
                    "ORDER BY score ASC LIMIT 2;"
                )
            )
            assert vector_rows, "vector search returned no rows"
            assert vector_rows[0][0] == 2, vector_rows
            print("Vector search: PASS")
            for row in vector_rows:
                print(f"  id={row[0]}, score={row[1]:.6f}")

            fts_rows = list(
                conn.execute(
                    "MATCH (n:Document) "
                    "RETURN n.id, n.text, bm25(n.text, 'sqlite') AS score "
                    "ORDER BY score ASC LIMIT 2;"
                )
            )
            assert fts_rows, "full-text search returned no rows"
            assert fts_rows[0][0] == 3, fts_rows
            print("FTS search: PASS")
            for row in fts_rows:
                print(f"  id={row[0]}, score={row[2]:.6f}, text={row[1]!r}")

            conn.close()
            conn = None
            endpoint = db.serve(
                port=_unused_tcp_port(),
                host="localhost",
                blocking=False,
                thread_num=4,
            )
            session = Session(endpoint, timeout="30s")

            vector_rows = list(
                session.execute(
                    "MATCH (n:Document) "
                    "RETURN n.id, "
                    "vector_distance_l2(n.embedding, [0.0, 0.9, 0.1]) AS score "
                    "ORDER BY score ASC LIMIT 2;",
                    access_mode="read",
                )
            )
            assert vector_rows, "TP vector search returned no rows"
            assert vector_rows[0][0] == 2, vector_rows
            print("TP vector search: PASS")
            for row in vector_rows:
                print(f"  id={row[0]}, score={row[1]:.6f}")

            updated_rows = list(
                session.execute(
                    "MATCH (n:Document {id: 1}) "
                    "SET n.embedding = [0.0, 0.9, 0.1], "
                    "n.text = 'updated hybrid search document' "
                    "RETURN n.id, n.text;",
                    access_mode="update",
                )
            )
            assert updated_rows == [[1, "updated hybrid search document"]], updated_rows
            print("TP vector and text update: PASS")

            updated_vector_rows = list(
                session.execute(
                    "MATCH (n:Document) "
                    "RETURN n.id, "
                    "vector_distance_l2(n.embedding, [0.0, 0.9, 0.1]) AS score "
                    "ORDER BY score ASC LIMIT 2;",
                    access_mode="read",
                )
            )
            assert updated_vector_rows, "updated TP vector search returned no rows"
            assert updated_vector_rows[0][0] == 1, updated_vector_rows
            assert abs(updated_vector_rows[0][1]) < 1e-6, updated_vector_rows
            print("Updated TP vector search: PASS")
            for row in updated_vector_rows:
                print(f"  id={row[0]}, score={row[1]:.6f}")

            updated_fts_rows = list(
                session.execute(
                    "MATCH (n:Document) "
                    "RETURN n.id, n.text, bm25(n.text, 'updated') AS score "
                    "ORDER BY score ASC LIMIT 2;",
                    access_mode="read",
                )
            )
            assert updated_fts_rows, "updated TP full-text search returned no rows"
            assert updated_fts_rows[0][0] == 1, updated_fts_rows
            assert (
                updated_fts_rows[0][1] == "updated hybrid search document"
            ), updated_fts_rows
            print("Updated TP FTS search: PASS")
            for row in updated_fts_rows:
                print(f"  id={row[0]}, score={row[2]:.6f}, text={row[1]!r}")
        finally:
            if session is not None:
                session.close()
            if conn is not None:
                conn.close()
            db.close()


if __name__ == "__main__":
    main()
