# Fonction de motif de graphe

En plus des diverses opérations fonctionnelles basées sur les données relationnelles introduites précédemment, Neug prend également spécifiquement en charge un ensemble de fonctions permettant d'opérer sur les données de nœuds, d'arêtes et de chemins générées par les motifs de graphe.

## Fonction de nœud

Fonction | Description | Exemple
---------|-------------|--------
ID() | Obtenir l'ID interne d'un nœud/une arête | Return (a) Return ID(a)
LABEL() | Obtenir le label d'un nœud/une arête | Match (a) Return LABEL(a)

## Fonction Rel

En plus des fonctions ID et LABEL, il existe les opérations fonctionnelles suivantes basées sur les arêtes :

Fonction | Description | Exemple
---------|-------------|--------
START_NODE() | Retourne le nœud de départ des données d'arête | Match ()-[b]->() Return START_NODE(b);
END_NODE() | Retourne le nœud d'arrivée des données d'arête | Match ()-[b]->() Return END_NODE(b);

## Fonction de Chemin Répétée

Fonction | Description | Exemple
---------|-------------|--------
NODES | Retourne tous les nœuds d'une relation récursive | Match (a)-[b*2..3]->() Return NODES(b);
RELS | Retourne toutes les relations d'une relation récursive | Match (a)-[b*2..3]->() Return RELS(b);
PROPERTIES | Retourne une propriété donnée des nœuds/relations | Match (a)-[b*2..3]->() Return PROPERTIES(nodes(b), 'name'), PROPERTIES(rels(b), 'weight');
IS_TRAIL | Vérifie si le chemin contient des relations répétées | Match (a)-[b*2..3]->() Return IS_TRAIL(b);
IS_ACYCLIC | Vérifie si le chemin contient des nœuds répétés | Match (a)-[b*2..3]->() Return IS_ACYCLIC(b);
LENGTH | Retourne le nombre de relations (longueur du chemin) dans une relation récursive | Match (a)-[b*2..3]->() Return LENGTH(b);