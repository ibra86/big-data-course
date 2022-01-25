-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
select c.name, count(*)
from film f
         left join film_category fc on f.film_id = fc.film_id
         left join category c on fc.category_id = c.category_id
group by 1
order by 2 desc
;
