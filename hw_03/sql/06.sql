-- 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
-- Отсортировать по количеству неактивных клиентов по убыванию.
select c2.city,
       count(*) filter ( where c.active=1 ) active_customers,
       count(*) filter ( where c.active=0 ) non_active_customers
from customer c
         left join address a on c.address_id = a.address_id
         left join city c2 on a.city_id = c2.city_id
group by 1
order by 3 desc, 2 desc
;