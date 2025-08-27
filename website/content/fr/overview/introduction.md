# Présentation de NeuG

Bienvenue dans NeuG (prononcé « new-gee »), une base de données graphe HTAP (Hybrid Transactional/Analytical Processing) conçue pour l'analyse embarquée de graphes haute performance et le traitement transactionnel en temps réel.

NeuG est construit pour répondre au besoin croissant de systèmes capables de gérer à la fois les charges de travail opérationnelles sur les graphes et les requêtes analytiques complexes sur les mêmes données, fournissant une plateforme unifiée pour les applications graphe modernes.

## Qu'est-ce que NeuG ?

NeuG est une base de données graphe qui combine le meilleur des deux mondes grâce à son **architecture unique en mode dual** :
- **Traitement Transactionnel (TP)** : Opérations graphe en temps réel avec des garanties ACID
- **Traitement Analytique (AP)** : Analytics graphe haute performance et correspondance de motifs
- **Modes de Connexion Dual** : Déploiement flexible en tant que système embarqué ou basé sur des services
- **Performance Optimisée** : Chaque mode est spécifiquement optimisé pour sa charge de travail cible

Construit sur un moteur central moderne en C++, NeuG fournit :
- Modèle de données Property Graph avec le langage de requête Cypher
- Stockage disque en colonnes avec listes d'adjacence compressées
- Traitement de requêtes vectorisé et parallélisé
- Gestion de transactions basée sur MVCC

## Pourquoi Choisir NeuG ?

### Architecture Unique en Mode Dual

La caractéristique distinctive de NeuG est sa **conception en mode dual** qui optimise les performances pour différents motifs de charge de travail graphe :

#### Mode Embedded - Optimisé pour l'Analytics (AP)
**Idéal pour** : Le pattern matching complexe, les calculs sur graphe complet, et les workloads analytiques
- **Chargement de données par batch** : Gère efficacement les imports de données à grande échelle
- **Performance de requêtes maximale** : Utilise tous les cœurs CPU disponibles pour les requêtes complexes
- **Utilisation complète des ressources** : Chaque requête peut tirer parti de l'ensemble des ressources système

**Cas d'usage typiques** :
- Analytics sur graphe et workloads data science
- Détection de patterns complexes et algorithmes sur graphe
- Traitement par batch et opérations ETL
- Recherche et analyse exploratoire de données

#### Mode Service - Optimisé pour les Transactions (TP)
**Idéal pour** : Applications en temps réel avec de fréquentes petites mises à jour et accès concurrents
- **Opérations de Lecture-Écriture Concurrentes** : Optimisé pour des modifications de données à haute fréquence et à petite échelle
- **Requêtes Ponctuelles** : Accès efficace à de petits sous-ensembles de données graphe
- **Support Multi-Session** : Gestion de plusieurs connexions concurrentes
- **Opérations à Faible Latence** : Temps de réponse rapides pour les charges de travail transactionnelles

**Cas d'Usage Typiques** :
- Systèmes de recommandation en temps réel
- Détection de fraude et gestion des risques
- Traitement de transactions en ligne (OLTP)

### Combinaison flexible des modes

La véritable puissance de NeuG réside dans la **combinaison flexible** des deux modes :

- **Intégration dans un pipeline de données** : Utilisez le mode embedded pour le chargement initial des données en masse, puis passez en mode service pour les requêtes opérationnelles
- **Workflows hybrides** : Exportez des données checkpointées depuis un service TP en cours d'exécution vers une copie séparée pour une analyse AP intensive
- **Développement vers production** : Développez et testez avec le mode embedded, déployez en tant que service pour la production
- **Séparation des charges de travail** : Exécutez les workloads AP sur des instances embedded dédiées tout en maintenant les services TP

> **Note** : Bien que le mode embedded puisse gérer des modifications à petite échelle, les écritures fréquentes bloqueront toutes les requêtes de lecture (et vice versa) en raison de son design de verrouillage exclusif. De même, le mode service peut exécuter de grandes requêtes pattern, mais il n'est pas recommandé d'utiliser tous les threads disponibles pour accélérer une seule requête puisque cela affamerait les autres requêtes concurrentes.

### Performance et Scalabilité
NeuG est conçu pour les workloads graphiques haute performance avec :
- Optimisation avancée des requêtes et planification d'exécution
- Opérations vectorisées pour les workloads analytiques
- Format de stockage efficace optimisé pour les traversées de graphes
- Parallélisme multi-cœur pour les requêtes complexes

### Flexibilité et Facilité d'Utilisation
- **Multi-Plateforme** : Fonctionne sur Linux et macOS sur les architectures x86 et ARM
- **Intégration Facile** : L'architecture embarquée élimine la surcharge de gestion de serveur
- **Changement de Mode** : Transition fluide entre les modes embarqué et service

### Fonctionnalités Enterprise-Ready
- **Transactions ACID** : Garanties ACID sérialisables pour les applications critiques
- **Accès Concurrent** : Optimisé pour les analytics lourds en lecture et les opérations intensives en écriture
- **Gestion des Erreurs** : Mécanismes complets de gestion et de récupération d'erreurs
- **Monitoring** : Surveillance des performances et profiling des requêtes intégrés

NeuG est développé par l'équipe [GraphScope](https://graphscope.io), un groupe de recherche leader d'Alibaba reconnu pour son expertise en calcul graphique et traitement de données à grande échelle. Le framework intègre des méthodologies avancées issues de l'expérience approfondie d'Alibaba dans le déploiement d'analyses graphiques pour résoudre des problématiques d'entreprise à grande échelle dans des domaines tels que l'e-commerce, l'optimisation de la chaîne d'approvisionnement et la détection de fraudes.