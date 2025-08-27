# Limit-Klausel

Limit wird verwendet, um die Anzahl der Ausgabeergebnisse zu steuern. Neben der alleinigen Verwendung kann es auch zusammen mit Order BY für TopK-Operationen genutzt werden. Neug unterstützt derzeit zwei Arten von Ausdrücken in Limit: 1. Eine einzelne Integer-Konstante. 2. Arithmetische Ausdrücke mit nur konstanten Parametern.

## Limit mit Integer-Wert

```
Match (a:person)
Return a.age
Limit 2;
```
Da keine Sortierung der Ausgabe erfolgt, können die Ergebnisse beliebige zwei Datensätze sein.

Ausgabe:
```
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

## Limit mit Integer-Ausdruck

```
Match (a:person)
Return a.age
Limit 1+1;
```

Ausgabe:
```
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

# Skip-Klausel

Limit kontrolliert die Anzahl der Ausgabeergebnisse und entspricht der Festlegung des Zeilennummernbereichs der Ausgabeergebnisse auf [0, upper_bound). Skip kontrolliert das Überspringen der ersten paar Zeilen der Ausgabeergebnisse und entspricht der Festlegung des Zeilennummernbereichs der Ausgabeergebnisse auf [lower_bound, +∞), wobei wir annehmen, dass die Zeilennummerierung bei 0 beginnt.

## Skip mit Integer-Wert

```
Match (a:person)
Return a.age
Skip 2;
```
Die Query wird verwendet, um die ersten beiden Ergebniszeilen zu überspringen.

Ausgabe:
```
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```

## Skip mit Integer-Ausdruck

```
Match (a:person)
Return a.age
Skip 1+1;
```

Ausgabe:
```
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```