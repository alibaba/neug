# Aggregatfunktionen

Aggregatfunktionen werden hauptsächlich verwendet, um aktuelle Daten zu gruppieren und Aggregationsoperationen auf Elementen innerhalb jeder Gruppe durchzuführen, wobei letztendlich nur ein einzelner Wert für jede Gruppe erzeugt wird. Die von Neug unterstützten Aggregatfunktionen sind wie folgt:

Funktion | Beschreibung | Kann mit DISTINCT verwendet werden | Beispiel
---------|-------------|---------------------------|--------
count | Gibt die Anzahl der Zeilen zurück | JA | Return count(a.name);
collect | Sammelt die Elemente in einer einzigen Liste | JA | Return collect(a.name);
min | Gibt den Minimalwert zurück | NEIN | Return min(a.age);
max | Gibt den Maximalwert zurück | NEIN | Return max(a.age);
sum | Summiert die Werte | NEIN | Return sum(a.age);
avg | Gibt den Durchschnittswert zurück | NEIN | Return avg(a.age);