-- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select a.first_name, a.last_name, sum(f.rental_duration) sum_rental_duration
from actor a
         left join film_actor fa on a.actor_id = fa.actor_id
         left join film f on fa.film_id = f.film_id
group by 1, 2
order by 3 desc
limit 10
;