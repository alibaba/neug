# Fonction d'agrégation

Les fonctions d'agrégation sont principalement utilisées pour regrouper les données actuelles et effectuer des opérations d'agrégation sur les éléments de chaque groupe, produisant finalement une seule valeur pour chaque groupe. Les fonctions d'agrégation supportées par Neug sont les suivantes :

Fonction | Description | Peut être utilisée avec DISTINCT | Exemple
---------|-------------|---------------------------|--------
count | retourne le nombre de lignes | OUI | Return count(a.name);
collect | collecte les éléments dans une seule liste | OUI | Return collect(a.name);
min | retourne la valeur minimale | NON | Return min(a.age);
max | retourne la valeur maximale | NON | Return max(a.age);
sum | fait la somme des valeurs | NON | Return sum(a.age);
avg | retourne la valeur moyenne | NON | Return avg(a.age);