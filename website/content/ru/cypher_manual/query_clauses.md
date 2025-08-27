# Клаузы запросов
В этой главе мы в основном расскажем о Clauses в Neug, связанных с запросами. В следующей таблице описаны типы и общее назначение этих операций:

Клауза | Описание
-------|------------
[MATCH](query_clauses/match_clause.md) | Поиск графовых паттернов
[WHERE](query_clauses/where_clause.md) | Фильтрация по свойствам
[WITH](query_clauses/with_clause.md) | Проекция или агрегация на основе свойств
[RETURN](query_clauses/return_clause.md) | Вывод результатов проекции или агрегации
[ORDER](query_clauses/order_clause.md) | Дополнительная сортировка промежуточных или выходных результатов
[SKIP](query_clauses/limit_clause.md) | Пропустить верхнюю часть результатов, определить нижнюю границу выводимых данных
[LIMIT](query_clauses/limit_clause.md) | Обрезать результаты, определить верхнюю границу выводимых данных
[UNION](query_clauses/union_clause.md) | Объединить несколько ветвей результатов с согласованной схемой
<!-- [UNWIND](query_clauses/unwind_clause.md) | Раскрытие списка результатов -->

В качестве примера мы будем использовать [modern graph](https://tinkerpop.apache.org/docs/current/reference/#graph-computing), чтобы показать, что конкретно делает каждая клауза.
<!-- Ниже представлена схема и диаграмма данных, соответствующая modern graph.  -->