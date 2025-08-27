# Cast Function

Die CAST-Funktion wird hauptsächlich für die Konvertierung zwischen verschiedenen Datentypen verwendet. Wir unterstützen derzeit nur die Typumwandlung für literale Konstantenwerte und werden in Zukunft zusätzlich Variablen, Eigenschaften und komplexere Ausdrücke unterstützen.

## Cast Literal

Neug erlaubt derzeit die folgenden Typumwandlungen:
- Gegenseitige Konvertierung zwischen numerischen Typen. Zu den numerischen Typen gehören: INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE. Jeder dieser Typen kann in jeden anderen konvertiert werden. Bei einem Überlauf wird eine Exception geworfen. Die nachfolgende Tabelle zeigt mögliche Exceptions, die auftreten können.
- Konvertierung zwischen String und numerischen Typen. Ein String kann in jeden numerischen Typ und umgekehrt konvertiert werden. Falls der konkrete Wert nicht konvertiert werden kann, wird eine Exception geworfen.

| Von \ Nach | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|------------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (wenn Wert < 0)        | ✅ Safe | ⚠️ May Overflow (wenn Wert < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (wenn Wert > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (wenn Wert < INT32_MIN oder > INT32_MAX) | ⚠️ May Overflow (wenn Wert < 0 oder > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (wenn Wert < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (wenn Wert > INT32_MAX)         | ⚠️ May Overflow (wenn Wert > UINT32_MAX)       | ⚠️ May Overflow (wenn Wert > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (wenn Wert < INT32_MIN oder > INT32_MAX) | ⚠️ May Overflow (wenn Wert < 0 oder > UINT32_MAX) | ⚠️ May Overflow (wenn Wert < INT64_MIN oder > INT64_MAX) | ⚠️ May Overflow (wenn Wert < 0 oder > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (wenn Wert < INT32_MIN oder > INT32_MAX) | ⚠️ May Overflow (wenn Wert < 0 oder > UINT32_MAX) | ⚠️ May Overflow (wenn Wert < INT64_MIN oder > INT64_MAX) | ⚠️ May Overflow (wenn Wert < 0 oder > UINT64_MAX) | ⚠️ May Overflow (wenn Wert < -FLT_MAX oder > FLT_MAX) | ✅ Safe |