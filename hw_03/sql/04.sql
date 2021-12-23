-- 4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select f.title
from film f
where not exists(select 1 from inventory i where f.film_id = i.film_id)
order by 1
;