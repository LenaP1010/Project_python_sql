CREATE DATABASE IF NOT EXISTS test; -- Создание БД "test" 
-- Создание таблицы "users" в БД
CREATE TABLE test.users ( 
    id UInt32,-- Идентификатор пользователя (тип UInt32)
    name String, -- Имя пользователя (тип String)
    email String, -- Электронная почта пользователя (тип String)
    created_at DateTime DEFAULT now() -- Дата и время создания записи (по умолчанию текущая дата и время)
) ENGINE = MergeTree() -- Использование движка MergeTree
ORDER BY id; -- Порядок сортировки по id
-- Создание таблицы "orders" в БД
CREATE TABLE test.orders (
    id UInt32, -- Идентификатор заказа (тип UInt32)
    user_id UInt32, -- Идентификатор пользователя, связанного с заказом (тип UInt32)
    total_price Float64, -- Общая цена заказа (тип Float64)
    created_at DateTime DEFAULT now() -- Дата и время создания записи (по умолчанию текущая дата и время)
) ENGINE = MergeTree()  -- Использование движка MergeTree
ORDER BY id; -- Порядок сортировки по id
-- Создание таблицы "order_items" в БД
CREATE TABLE test.order_items (
    id UInt32, -- Идентификатор товара в заказе (тип UInt32)
    order_id UInt32, -- Идентификатор заказа, к которому относится товар (тип UInt32)
    product_name String, -- Название товара (тип String)
    price Float64, -- Цена товара (тип Float64)
    quantity UInt32 -- Количество товара в заказе (тип UInt32)
) ENGINE = MergeTree() -- Использование движка MergeTree
ORDER BY id; Порядок сортировки по id
SHOW TABLES FROM test; -- Для проверки показываем все таблицы в БД "test". Зоплнено тестовыми данными (код в generate_data.py)

-- Запрос 1 - Найти общее количество заказов каждого пользователя, который сделал более 10 заказов. 
SELECT user_id, COUNT(*) AS order_count -- Извлекаем 'user_id' и общее количество заказов для каждого пользователя
FROM test.orders -- Из таблицы 'orders', которая находится в базе данных 'test'
GROUP BY user_id  -- Группируем по 'user_id', чтобы подсчитать количество заказов для каждого пользователя
HAVING order_count > 10; -- Ограничиваем результаты, показывая только пользователей с более чем 10 заказами

-- Запрос 2 - Найти средний размер заказа для каждого пользователя за последний месяц
SELECT user_id, AVG(total_price) AS avg_order_size -- Извлекаем 'user_id' и среднюю сумму заказов для каждого пользователя
FROM test.orders -- Из таблицы 'orders', которая находится в базе данных 'test'
WHERE created_at >= now() - INTERVAL 1 MONTH -- Фильтруем заказы, чтобы учитывать только те, что сделаны за последний месяц
GROUP BY user_id; -- Группируем по 'user_id', чтобы вычислить средний размер заказа для каждого пользователя

-- Запрос 3 - Найти средний размер заказа за каждый месяц в текущем году и сравнить его с средним размером заказа за соответствующий месяц в прошлом году
-- Создаем временную таблицу с номерами месяцев
WITH months AS ( 
    SELECT number + 1 AS month -- Нумеруем месяцы, начиная с 1
    FROM system.numbers -- Используем таблицу 'numbers'
    WHERE number < 12 -- Указываем, чтобы извлечь номера менее 12 
),
-- Создаем временную таблицу для хранения данных о заказах текущего года
current_year_data AS (
    SELECT toMonth(created_at) AS month, AVG(total_price) AS avg_order_size -- Получаем номер месяца из 'created_at'
    FROM test.orders -- Из таблицы 'orders', которая находится в базе данных 'test'
    WHERE created_at >= toStartOfYear(today()) -- Указываем, что необходимо учитывать заказы с начала текущего года
        AND created_at < toStartOfYear(today() + INTERVAL 1 YEAR) -- И до начала следующего года
    GROUP BY month -- Группируем данные по месяцу для вычисления среднего размера в каждом месяце
),
-- Создаем временную таблицу для хранения данных о заказах прошлого года
last_year_data AS ( 
    SELECT toMonth(created_at) AS month, AVG(total_price) AS avg_order_size -- Получаем номер месяца из 'created_at' и рассчитываем средний размер заказа
    FROM test.orders -- Из таблицы 'orders', которая находится в базе данных 'test'
    WHERE created_at >= toStartOfYear(today() - INTERVAL 1 YEAR) -- Указываем, чтобы учитывать заказы с начала прошлого года
        AND created_at < toStartOfYear(today()) -- И до начала текущего года
    GROUP BY month -- Группируем данные по месяцу для вычисления среднего размера в каждом месяце
)
SELECT m.month, -- Извлекаем номер месяца из временной таблицы 'months'
    COALESCE(c.avg_order_size, 0) AS avg_order_size_current_year, -- Получаем средний размер заказа за текущий год или 0, если данных нет
    COALESCE(l.avg_order_size, 0) AS avg_order_size_last_year, -- Получаем средний размер заказа за прошлый год или 0, если данных нет
    COALESCE(c.avg_order_size, 0) - COALESCE(l.avg_order_size, 0) AS comparison --Сравниваем средние размеры заказов текущего и прошлого года
FROM months m -- Используем временную таблицу 'months'
LEFT JOIN current_year_data c ON m.month = c.month -- Соединяем с помощью LEFT JOIN (левое соединение) с данными текущего года по месяцу
LEFT JOIN last_year_data l ON m.month = l.month -- Соединяем с помощью LEFT JOIN с данными прошлого года по месяцу
ORDER BY m.month; -- Сортируем результаты по номеру месяца

/*Запрос 4 - Найти 10 пользователей, у которых наибольшее количество заказов за последний год, и для каждого из них найти средний размер заказа
за последний месяц.*/
-- Создаем временную таблицу для хранения пользователей с наибольшим количеством заказов за последний год
WITH last_year_orders AS (
    SELECT user_id, COUNT(*) AS order_count -- Извлекаем 'user_id' и общее количество заказов для каждого пользователя
    FROM test.orders -- Из таблицы 'orders', которая находится в базе данных 'test'
    WHERE created_at >= now() - INTERVAL 1 YEAR -- Фильтруем заказы, чтобы учитывать только те, что сделаны за последний год
    GROUP BY user_id -- Группируем по 'user_id', чтобы подсчитать количество заказов для каждого пользователя
    ORDER BY order_count DESC -- Сортируем результаты по количеству заказов в порядке убывания
    LIMIT 10 -- Ограничиваем вывод до 10 пользователей
)
SELECT o.user_id, AVG(o.total_price) AS avg_order_size_last_month -- Извлекаем 'user_id' пользователей и рассчитываем средний размер заказа за последний месяц
FROM last_year_orders AS l -- Из временной таблицы 'last_year_orders'
INNER JOIN test.orders AS o ON l.user_id = o.user_id -- Выполняем внутреннее соединение с таблицей 'orders' по 'user_id'
WHERE o.created_at >= now() - INTERVAL 1 MONTH -- Фильтруем заказы, чтобы учитывать только те, что сделаны за последний месяц
GROUP BY o.user_id; -- Группируем результаты по 'user_id'

-- ВАРИАНТЫ С "ПЛОХИМИ" ЗАПРОСАМИ

--Запрос 1 ("плохой") - Найти общее количество заказов каждого пользователя, который сделал более 10 заказов.
SELECT user_id, COUNT(*) AS order_count 
FROM test.orders 
WHERE user_id IN (SELECT user_id FROM test.orders) /*лишняя операция, которая по сути не влияет на результаты, так как уже считаем заказы для всех пользователей. 
Это делает запрос избыточным и менее эффективным.*/
GROUP BY user_id 
HAVING order_count > 10;

--Запрос 2 ("плохой") - Найти средний размер заказа для каждого пользователя за последний месяц
SELECT user_id, AVG(total_price) AS avg_order_size
FROM test.orders
WHERE created_at >= '2025-03-01' AND created_at < '2025-04-01' -- Указаны фиксированные даты вместо динамического интервала, что делает запрос неуниверсальным
GROUP BY user_id
ORDER BY user_id
LIMIT 100; -- Лимитирование результата, что не имеет смысла 

-- Запрос 3 ("плохой") - Найти средний размер заказа за каждый месяц в текущем году и сравнить его с средним размером заказа за соответствующий месяц в прошлом году
-- Избыток подзапросов и лишних операций
WITH months AS (
    SELECT number + 1 AS month
    FROM system.numbers
    WHERE number < 12
),
-- Добавляем ненужный уровень вложенности в запрос на получение текущего года.
current_year_data AS (
    SELECT month, AVG(total_price) AS avg_order_size
    FROM (
        SELECT toMonth(created_at) AS month, total_price
        FROM test.orders
        WHERE created_at >= toStartOfYear(today()) 
            AND created_at < toStartOfYear(today() + INTERVAL 1 YEAR)
    )
    GROUP BY month
),
last_year_data AS (
    SELECT month, AVG(total_price) AS avg_order_size
    FROM (
        SELECT toMonth(created_at) AS month, total_price
        FROM test.orders
        WHERE created_at >= toStartOfYear(today() - INTERVAL 1 YEAR) 
            AND created_at < toStartOfYear(today())
    )
    GROUP BY month
)
SELECT m.month,
    IFNULL(c.avg_order_size, -1) AS avg_order_size_current_year,  -- Используем -1 вместо 0, что может быть запутанным
    IFNULL(l.avg_order_size, -1) AS avg_order_size_last_year,  -- Тоже -1 вместо 0
    IFNULL(c.avg_order_size, -1) - IFNULL(l.avg_order_size, -1) AS comparison
FROM months m
LEFT JOIN current_year_data c ON m.month = c.month
LEFT JOIN last_year_data l ON m.month = l.month
ORDER BY m.month, avg_order_size_current_year DESC; -- Не имеет смысла сортировать по avg_order_size_current_year

/*Запрос 4 ("плохой") - Найти 10 пользователей, у которых наибольшее количество заказов за последний год, и для каждого из них найти средний размер заказа
за последний месяц.*/
--Избыточные подзапросы и неоптимальные операции
WITH last_year_orders AS (
    SELECT user_id, COUNT(*) AS order_count
    FROM (
        SELECT user_id, created_at
        FROM test.orders
        WHERE created_at >= now() - INTERVAL 1 YEAR
    )
    GROUP BY user_id
    ORDER BY order_count DESC
    LIMIT 10
)
SELECT l.user_id,
    AVG(o.total_price) AS avg_order_size_last_month
FROM last_year_orders AS l
JOIN (
    SELECT user_id, total_price
    FROM test.orders
    WHERE created_at >= now() - INTERVAL 1 MONTH
) AS o ON l.user_id = o.user_id
GROUP BY l.user_id
ORDER BY avg_order_size_last_month DESC; -- Сортировка лишняя для такого запроса



