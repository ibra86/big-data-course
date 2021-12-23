-- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
-- и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
with t as (
    select c.name category, c3.city, f.rental_duration
    from film f
             left join film_category fc on f.film_id = fc.film_id
             left join category c on fc.category_id = c.category_id
             left join inventory i on f.film_id = i.film_id
             left join store s on i.store_id = s.store_id
             left join customer c2 on s.store_id = c2.store_id
             left join address a on c2.address_id = a.address_id
             left join city c3 on a.city_id = c3.city_id
),
     a_city as (
         select category, city, sum(rental_duration) total_duration
         from t
         where city ilike 'a%'
         group by 1, 2
     ),
     a_city_rk as (
         select *, dense_rank() over (order by total_duration desc) as rk
         from a_city
     ),
     dash_city as (
         select category, city, sum(rental_duration) total_duration
         from t
         where city like '%-%'
         group by 1, 2
     ),
     dash_city_rk as (
         select *, dense_rank() over (order by total_duration desc) as rk
         from dash_city
     )
select category, city, total_duration
from a_city_rk
where rk = 1
union
select category, city, total_duration
from dash_city_rk
where rk = 1
order by 3 desc
;