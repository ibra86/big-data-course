-- 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with t as
         (select a.first_name, a.last_name, count(*) total_count
          from actor a
                   left join film_actor fa on a.actor_id = fa.actor_id
                   left join film f on fa.film_id = f.film_id
                   left join film_category fc on f.film_id = fc.film_id
                   left join category c on fc.category_id = c.category_id
          where c.name = 'Children'
          group by 1, 2),
     t_rk as (
         select *, dense_rank() over (order by total_count desc) rk
         from t)
select first_name, last_name, rk
from t_rk
where rk<=3
;