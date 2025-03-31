import random
from faker import Faker
from clickhouse_driver import Client

# Настройки для подключения к ClickHouse
clickhouse_host = 'localhost'  # Хост ClickHouse
clickhouse_database = 'test' # Название БД
client = Client(host=clickhouse_host, database=clickhouse_database) # Создание клиента для подключения к БД

# Инициализация Faker для генерации фейковых данных
fake = Faker()

# Количество записей, которые будут созданы
num_users = 1000000
num_orders = 1000000
num_order_items = 3000000  

# Создание пользователей
user_data = [] # Список для хранения данных пользователей
for user_id in range(num_users):
    user_data.append((user_id, fake.name(), fake.email()))     # Генерация фейковых данных: id, имя, email

# Вставка пользователей в таблицу users
client.execute('INSERT INTO test.users (id, name, email) VALUES', user_data)

# Создание заказов
order_data = [] # Список для хранения данных заказов
for order_id in range(num_orders): 
    user_id = random.randint(0, num_users - 1)  # Случайный идентификатор пользователя
    total_price = round(random.uniform(10, 500), 2)  # Случайная цена заказа
    order_data.append((order_id, user_id, total_price)) # Добавление данных заказа в список

# Вставка заказов в таблицу orders
client.execute('INSERT INTO test.orders (id, user_id, total_price) VALUES', order_data)

# Создание позиций заказов
order_item_data = [] # Список для хранения данных позиций заказов
for item_id in range(num_order_items): 
    order_id = random.randint(0, num_orders - 1)  # Случайный идентификатор заказа
    product_name = fake.word()
    price = round(random.uniform(1, 100), 2)  # Случайная цена товара
    quantity = random.randint(1, 10)  # Случайное количество товаров
    order_item_data.append((item_id, order_id, product_name, price, quantity))

# Вставка позиций заказов в таблицу order_items
client.execute('INSERT INTO test.order_items (id, order_id, product_name, price, quantity) VALUES', order_item_data)
# Вывод сообщения об успешной загрузке данных
print("Данные успешно загружены в ClickHouse.")