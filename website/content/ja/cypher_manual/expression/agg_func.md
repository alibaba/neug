# 集約関数

集約関数は、主に現在のデータをグループ化し、各グループ内の要素に対して集約操作を実行するために使用され、最終的に各グループに対して単一の値のみを生成します。Neugがサポートする集約関数は以下の通りです：

関数 | 説明 | DISTINCTとの併用可否 | 例
-----|------|-------------------|----
count | 行数を返す | YES | Return count(a.name);
collect | 要素を単一のリストに収集する | YES | Return collect(a.name);
min | 最小値を返す | NO | Return min(a.age);
max | 最大値を返す | NO | Return max(a.age);
sum | 値を合計する | NO | Return sum(a.age);
avg | 平均値を返す | NO | Return avg(a.age);