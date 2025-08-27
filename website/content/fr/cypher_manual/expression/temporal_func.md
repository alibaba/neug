# Fonctions Temporelles

Les fonctions temporelles sont un ensemble spécialisé de fonctions conçues pour les types de données date et intervalle. Elles fournissent des capacités pour construire des types date/intervalle à partir de littéraux de chaîne et pour extraire des champs spécifiques à partir de valeurs temporelles. Le tableau suivant liste les fonctions disponibles et leur utilisation.

## Référence des fonctions

| Fonction | Description | Exemple | Type de retour |
|----------|-------------|---------|-------------|
| `DATE()` | Construit une valeur de type date à partir d'une chaîne de caractères | `DATE('2012-01-01')` | `DATE` |
| `TIMESTAMP()` | Construit une valeur de type timestamp à partir d'une chaîne de caractères | `TIMESTAMP('1926-11-21 13:22:19')` | `TIMESTAMP` |
| `INTERVAL()` | Construit une valeur de type interval à partir d'une chaîne de caractères | `INTERVAL('3 DAYS')` | `INTERVAL` |
| `date_part(part, date)` | Extrait une partie spécifique d'une valeur de type date | `date_part('year', DATE('1995-11-02'))` | `INTEGER` |
| `date_part(part, timestamp)` | Extrait une partie spécifique d'une valeur de type timestamp | `date_part('month', TIMESTAMP('1926-11-21 13:22:19'))` | `INTEGER` |
| `date_part(part, interval)` | Extrait une partie spécifique d'une valeur de type interval | `date_part('days', INTERVAL('1 days'))` | `INTEGER` |