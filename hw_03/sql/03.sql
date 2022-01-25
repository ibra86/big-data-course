-- 3. вывести категорию фильмов, на которую потратили больше всего денег.
with t as (
    select c.name, r.rental_id, r.staff_id, r.customer_id
    from category c
             left join film_category fc on c.category_id = fc.category_id
             left join film f on fc.film_id = f.film_id
             left join inventory i on f.film_id = i.film_id
             left join rental r on i.inventory_id = r.inventory_id
),
     payment_rental as (
         select t.name, p.amount, 'rental' "type"
         from t left join payment p on t.rental_id = p.rental_id
     ),
     payment_customer as (
         select t.name, p.amount, 'customer' "type"
         from t left join payment p on t.customer_id = p.customer_id
     ),
     payment_staff as (
         select t.name, p.amount, 'staff' "type"
         from t left join payment p on t.staff_id = p.customer_id
     ),
     payment_all as (
         select * from payment_rental
         union
         select * from payment_customer
         union
         select * from payment_staff
     )
select name, sum(amount) sum_amount
from payment_all
group by 1
order by 2 desc
limit 1
;