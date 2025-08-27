# Query Clauses
Dans ce chapitre, nous présentons principalement les opérations de Clauses liées aux requêtes Neug. Le tableau suivant résume les types et les objectifs généraux de ces opérations :

Clause | Description
-------|------------
[MATCH](query_clauses/match_clause.md) | Trouver un motif dans le graphe
[WHERE](query_clauses/where_clause.md) | Filtrer en fonction des propriétés
[WITH](query_clauses/with_clause.md) | Projection ou agrégation basée sur les propriétés
[RETURN](query_clauses/return_clause.md) | Projeter ou agréger les résultats en sortie
[ORDER](query_clauses/order_clause.md) | Trier davantage les résultats intermédiaires ou en sortie
[SKIP](query_clauses/limit_clause.md) | Ignorer une portion initiale des résultats, définir la borne inférieure des résultats en sortie
[LIMIT](query_clauses/limit_clause.md) | Tronquer les résultats, définir la borne supérieure des résultats en sortie
[UNION](query_clauses/union_clause.md) | Fusionner plusieurs résultats de branches ayant un schéma compatible
<!-- [UNWIND](query_clauses/unwind_clause.md) | Déplier un résultat de type liste -->

Nous utiliserons le [graphe modern](https://tinkerpop.apache.org/docs/current/reference/#graph-computing) comme exemple pour illustrer précisément ce que fait chaque Clause.
<!-- Ci-dessous se trouve le schéma et le diagramme des données correspondant au graphe modern. -->