# Fonction CAST

La fonction CAST est principalement utilisée pour convertir entre différents types de données. Nous ne prenons actuellement en charge que la conversion de type pour les valeurs constantes littérales, et nous ajouterons la prise en charge des variables, des propriétés et d'expressions plus complexes dans le futur.

## Cast Literal

Neug permet actuellement les conversions de type suivantes :
- Conversion mutuelle entre les types numériques. Les types numériques incluent : INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE. N'importe quel type numérique peut être converti vers un autre. Si un dépassement (overflow) se produit, une exception sera levée. Le tableau ci-dessous montre les exceptions possibles que vous pouvez rencontrer.
- Conversion entre String et les types numériques. Un String peut être converti vers ou depuis n'importe quel type numérique. Si la valeur spécifique ne peut pas être convertie, une exception sera levée.

| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (si value < 0)        | ✅ Safe | ⚠️ May Overflow (si value < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (si value > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (si value < INT32_MIN ou > INT32_MAX) | ⚠️ May Overflow (si value < 0 ou > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (si value < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (si value > INT32_MAX)         | ⚠️ May Overflow (si value > UINT32_MAX)       | ⚠️ May Overflow (si value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (si value < INT32_MIN ou > INT32_MAX) | ⚠️ May Overflow (si value < 0 ou > UINT32_MAX) | ⚠️ May Overflow (si value < INT64_MIN ou > INT64_MAX) | ⚠️ May Overflow (si value < 0 ou > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (si value < INT32_MIN ou > INT32_MAX) | ⚠️ May Overflow (si value < 0 ou > UINT32_MAX) | ⚠️ May Overflow (si value < INT64_MIN ou > INT64_MAX) | ⚠️ May Overflow (si value < 0 ou > UINT64_MAX) | ⚠️ May Overflow (si value < -FLT_MAX ou > FLT_MAX) | ✅ Safe |