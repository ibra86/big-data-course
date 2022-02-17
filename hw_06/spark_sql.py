from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pg_url = 'jdbc:postgresql://localhost:5432/pagila?user=pguser&password=secret'

spark = SparkSession.builder \
    .appName('hw_06') \
    .master('local') \
    .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.2.jar') \
    .config('spark.ui.enabled', 'false') \
    .getOrCreate()


def ff_1():
    '''
    -- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.

    select c.name, count(*)
    from film f
             left join film_category fc on f.film_id = fc.film_id
             left join category c on fc.category_id = c.category_id
    group by 1
    order by 2 desc
    ;
    '''

    df_f = spark.read.jdbc(url=pg_url, table='film')
    df_fc = spark.read.jdbc(url=pg_url, table='film_category')
    df_c = spark.read.jdbc(url=pg_url, table='category')

    df = df_f \
        .join(df_fc, 'film_id', 'left') \
        .join(df_c, 'category_id', 'left') \
        .groupBy(df_c.name) \
        .count().alias('category_count') \
        .orderBy(F.col('count'), ascending=False) \
        .toPandas()

    return df


def ff_2():
    '''
    -- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

    select a.first_name, a.last_name, sum(f.rental_duration) sum_rental_duration
    from actor a
             left join film_actor fa on a.actor_id = fa.actor_id
             left join film f on fa.film_id = f.film_id
    group by 1, 2
    order by 3 desc
    limit 10
    ;
    '''

    df_a = spark.read.jdbc(url=pg_url, table='actor')
    df_fa = spark.read.jdbc(url=pg_url, table='film_actor')
    df_f = spark.read.jdbc(url=pg_url, table='film')

    df = df_a \
        .join(df_fa, on='actor_id', how='left') \
        .join(df_f, on='film_id', how='left') \
        .groupBy(df_a.first_name, df_a.last_name) \
        .agg(F.sum('rental_duration').alias('sum_rental_duration')) \
        .orderBy(F.col('sum_rental_duration'), ascending=False) \
        .limit(10) \
        .toPandas()

    return df


def ff_3():
    '''
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
    '''

    df_c = spark.read.jdbc(url=pg_url, table='category')
    df_fc = spark.read.jdbc(url=pg_url, table='film_category')
    df_f = spark.read.jdbc(url=pg_url, table='film')
    df_i = spark.read.jdbc(url=pg_url, table='inventory')
    df_r = spark.read.jdbc(url=pg_url, table='rental')
    df_p = spark.read.jdbc(url=pg_url, table='payment')

    df_t = df_c \
        .join(df_fc, on='category_id', how='left') \
        .join(df_f, on='film_id', how='left') \
        .join(df_i, on='film_id', how='left') \
        .join(df_r, on='inventory_id', how='left')

    df_pr = df_t \
        .join(df_p, on='rental_id', how='left') \
        .select(df_t.name, df_p.amount) \
        .withColumn('type', F.lit('rental'))

    df_pc = df_t \
        .join(df_p, on='customer_id', how='left') \
        .select(df_t.name, df_p.amount) \
        .withColumn('type', F.lit('customer'))

    df_ps = df_t \
        .join(df_p, on=[df_t.staff_id == df_p.customer_id], how='left') \
        .select(df_t.name, df_p.amount) \
        .withColumn('type', F.lit('staff'))

    df_p_all = df_pr \
        .union(df_pc) \
        .union(df_ps) \
        .distinct()

    df = df_p_all \
        .groupBy('name') \
        .agg(F.sum('amount').alias('sum_amount')) \
        .orderBy('sum_amount', ascending=False) \
        .limit(1) \
        .toPandas()

    return df


def ff_4():
    '''
    -- 4. вывести названия фильмов, которых нет в inventory.

    select f.title
    from film f
    where not exists(select 1 from inventory i where f.film_id = i.film_id)
    order by 1
    ;
    '''
    df_f = spark.read.jdbc(url=pg_url, table='film')
    df_i = spark.read.jdbc(url=pg_url, table='inventory')

    df = df_f \
        .join(df_i, on='film_id', how='left_anti') \
        .select(df_f.title) \
        .toPandas()

    return df


def ff_5():
    '''
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
    '''

    df_a = spark.read.jdbc(url=pg_url, table='actor')
    df_fa = spark.read.jdbc(url=pg_url, table='film_actor')
    df_f = spark.read.jdbc(url=pg_url, table='film')
    df_fc = spark.read.jdbc(url=pg_url, table='film_category')
    df_c = spark.read.jdbc(url=pg_url, table='category')

    df = df_a \
        .join(df_fa, on='actor_id', how='left') \
        .join(df_f, on='film_id', how='left') \
        .join(df_fc, on='film_id', how='left') \
        .join(df_c, on='category_id', how='left') \
        .where(df_c.name == 'Children') \
        .groupBy(df_a.first_name, df_a.last_name) \
        .count() \
        .withColumnRenamed('count', 'total_count') \
        .withColumn('rk', F.dense_rank().over(Window.orderBy(F.desc('total_count')))) \
        .where(F.col('rk') <= 3) \
        .select('first_name', 'last_name', 'rk') \
        .toPandas()

    return df


def ff_6():
    '''
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
    '''

    df_cu = spark.read.jdbc(url=pg_url, table='customer')
    df_a = spark.read.jdbc(url=pg_url, table='address')
    df_c = spark.read.jdbc(url=pg_url, table='city')

    df = df_cu \
        .join(df_a, on='address_id', how='left') \
        .join(df_c, on='city_id', how='left') \
        .groupBy(df_c.city) \
        .agg(
            F.count(F.when(df_cu.active == 1, True)).alias('active_customers'),
            F.count(F.when(df_cu.active == 0, True)).alias('non_active_customers')
        ) \
        .orderBy(['non_active_customers', 'active_customers', 'city'], ascending=[0, 0, 1]) \
        .toPandas()

    return df


def ff_7():
    '''
    -- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
    -- (customer.address_id в этом city), и которые начинаются на букву “a”.
    -- То же самое сделать для городов в которых есть символ “-”.

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
    '''

    df_f = spark.read.jdbc(url=pg_url, table='film')
    df_fc = spark.read.jdbc(url=pg_url, table='film_category')
    df_c = spark.read.jdbc(url=pg_url, table='category')
    df_i = spark.read.jdbc(url=pg_url, table='inventory')
    df_s = spark.read.jdbc(url=pg_url, table='store')
    df_cu = spark.read.jdbc(url=pg_url, table='customer')
    df_a = spark.read.jdbc(url=pg_url, table='address')
    df_ci = spark.read.jdbc(url=pg_url, table='city')

    df_t = df_f \
        .join(df_fc, on=[df_f.film_id == df_fc.film_id], how='left') \
        .join(df_c, on=[df_fc.category_id == df_c.category_id], how='left') \
        .join(df_i, on=[df_f.film_id == df_i.film_id], how='left') \
        .join(df_s, on=[df_i.store_id == df_s.store_id], how='left') \
        .join(df_cu, on=[df_s.store_id == df_cu.store_id], how='left') \
        .join(df_a, on=[df_cu.address_id == df_a.address_id], how='left') \
        .join(df_ci, on=[df_a.city_id == df_ci.city_id], how='left') \
        .select(df_c.name, df_ci.city, df_f.rental_duration) \
        .withColumnRenamed('name', 'category')

    df_ci_a = df_t \
        .where(F.lower(F.col('city')).like('a%')) \
        .groupBy('category', 'city') \
        .agg(F.sum('rental_duration').alias('total_duration'))

    df_ci_rk = df_ci_a \
        .withColumn('rk', F.dense_rank().over(Window.orderBy(F.desc('total_duration'))))

    df_ci_d = df_t \
        .where(F.col('city').like('%-%')) \
        .groupBy('category', 'city') \
        .agg(F.sum('rental_duration').alias('total_duration'))

    df_ci_d_rk = df_ci_d \
        .withColumn('rk', F.dense_rank().over(Window.orderBy(F.desc('total_duration'))))

    df = df_ci_rk.where(F.col('rk') == 1) \
        .union(df_ci_d_rk.where(F.col('rk') == 1)) \
        .distinct() \
        .orderBy('rk', ascending=False) \
        .toPandas()

    return df


if __name__ == '__main__':
    df_1 = ff_1()
    df_2 = ff_2()
    df_3 = ff_3()
    df_4 = ff_4()
    df_5 = ff_5()
    df_6 = ff_6()
    df_7 = ff_7()
