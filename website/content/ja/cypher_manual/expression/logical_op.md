# 論理演算子

Neug は現在、AND、OR、NOT の3つの論理演算子をサポートしており、SQL の「3値論理」に従って NULL 値を特別に処理します。具体的な真理値表は以下の通りです。

## AND 真理値表

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| TRUE     | TRUE     | TRUE   |
| TRUE     | FALSE    | FALSE  |
| TRUE     | NULL     | NULL   |
| FALSE    | TRUE     | FALSE  |
| FALSE    | FALSE    | FALSE  |
| FALSE    | NULL     | FALSE  |
| NULL     | TRUE     | NULL   |
| NULL     | FALSE    | FALSE  |
| NULL     | NULL     | NULL   |

## OR 真理値表

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| TRUE     | TRUE     | TRUE   |
| TRUE     | FALSE    | TRUE   |
| TRUE     | NULL     | TRUE   |
| FALSE    | TRUE     | TRUE   |
| FALSE    | FALSE    | FALSE  |
| FALSE    | NULL     | NULL   |
| NULL     | TRUE     | TRUE   |
| NULL     | FALSE    | NULL   |
| NULL     | NULL     | NULL   |

## NOT 真理値表

| オペランド | 結果   |
|------------|--------|
| TRUE       | FALSE  |
| FALSE      | TRUE   |
| NULL       | NULL   |