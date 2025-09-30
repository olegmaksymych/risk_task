**Тестове Завдання для Кандидата: Data Engineer**

### **Завдання 1: Робота з Python та API**

#### **1.1. Python та JSON**
Напишіть Python-скрипт, який:
1. Виконає **GET-запит** до публічного API https://jsonplaceholder.typicode.com/posts.
2. Обробить **JSON-відповідь**, отриману від API.
3. Знайде всі пости, де userId дорівнює **1**.
4. Збереже ці пости у новий JSON-файл з назвою user_1_posts.json.

    ```python
import requests  
import json  
  
def fetch_and_filter_posts():  
    # 1. GET-request to API  
    url = "https://jsonplaceholder.typicode.com/posts"  
    response = requests.get(url)  
  
    # Checking the status of the answer  
    if response.status_code != 200:  
        raise Exception(f"Error of the request: {response.status_code}")  
  
    # 2. Receiving JSON-answer
    posts = response.json()  
  
    # 3. Filtration of the posts by userId == 1  
    user_1_posts = [post for post in posts if post.get("userId") == 1]  
  
    # 4. Writing the results into new file  
    with open("user_1_posts.json", "w", encoding="utf-8") as f:  
        json.dump(user_1_posts, f, indent=4, ensure_ascii=False)  
  
    print(f"Saved {len(user_1_posts)} posts into 'user_1_posts.json'")  
  
if __name__ == "__main__":  
    fetch_and_filter_posts() 
```

#### **1.2. Імітація AWS Athena та Pandas**
Напишіть функцію, яка імітує підключення до AWS Athena та виконання запиту. Ваш скрипт повинен:

1. Імітувати підключення до AWS Athena, прописавши випадкові облікові дані (наприклад, aws_access_key_id, aws_secret_access_key).
2. Виконати запит SELECT * FROM your_database.your_table LIMIT 100.
3. Зберегти результат цього запиту у **Pandas DataFrame**.
4. Вивести перші **10 рядків** з отриманого DataFrame.

**Примітка:** Оскільки у вас немає доступу до реальних AWS-ресурсів, сфокусуйтеся на демонстрації того, як би виглядав процес: імітуйте отримання даних і завантаження їх у DataFrame.

```python
import os  
import sys  
from dotenv import load_dotenv  
from pyathena import connect  
from pyathena.pandas.cursor import PandasCursor  
  
# -------------------------------  
# Load AWS credentials from .env  
# -------------------------------  
load_dotenv()  
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")  
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")  
AWS_REGION = os.getenv("AWS_REGION")  
S3_OUTPUT = os.getenv("S3_OUTPUT")  # must end with '/'  
  
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_OUTPUT]):  
    print("❌ Error: check .env variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_OUTPUT)")  
    sys.exit(1)  
  
# -------------------------------  
# Connect to Athena  
# -------------------------------  
try:  
    conn = connect(  
        aws_access_key_id=AWS_ACCESS_KEY_ID,  
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,  
        region_name=AWS_REGION,  
        s3_staging_dir=S3_OUTPUT,  
        cursor_class=PandasCursor  # ensures results are returned as Pandas DataFrame  
    )  
    print("✅ Athena connection successful")  
except Exception as e:  
    print("❌ Failed to connect to Athena:", e)  
    sys.exit(2)  
  
# -------------------------------  
# Execute query  
# -------------------------------  
DATABASE = os.getenv("DATABASE")    # replace with your database  
TABLE = "orders"        # replace with your table  
LIMIT = 100  
  
QUERY = f"SELECT * FROM {DATABASE}.{TABLE} LIMIT {LIMIT}"  
  
try:  
    cursor = conn.cursor()  
    cursor.execute(QUERY)  
    df = cursor.as_pandas()  # result as Pandas DataFrame  
    print(f"✅ Retrieved {len(df)} rows. First 10 rows:")  
    print(df.head(10).to_string(index=False))  
except Exception as e:  
    print("❌ Query execution failed:", e)  
    sys.exit(3)  
  
# -------------------------------  
# Save results to CSV  
# -------------------------------  
out_csv = f"athena_{DATABASE}_{TABLE}_limit{LIMIT}.csv"  
df.to_csv(out_csv, index=False)  
print(f"File saved: {out_csv}")    
```


---

### **Завдання 2: SQL-Запити**
Надайте SQL-запити для наступних завдань. Зверніть увагу на ефективність та правильне використання JOIN-ів.
**Схема Бази Даних:**
· users: user_id (int), first_name (string), last_name (string)
· transactions: transaction_id (int), user_id (int), status (string)
o status може бути Success або Failed.
· fraud_transactions: transaction_id (int), is_fraud (bool)
o Містить всі транзакції та їхній статус фроду (True/False).

#### **2.1. Успішні та не фродові транзакції**
Напишіть запит, який виведе список всіх **користувачів** (user_id, first_name, last_name) та айдішники (transaction_id) їхніх **успішних та не фродових транзакцій**.


```sql
SELECT 
    u.user_id,
    u.first_name,
    u.last_name,
    t.transaction_id
FROM users u
JOIN transactions t
    ON u.user_id = t.user_id
JOIN fraud_transactions f
    ON t.transaction_id = f.transaction_id
WHERE t.status = 'Success'
  AND f.is_fraud = FALSE
ORDER BY u.user_id, t.transaction_id;
```


#### **2.2. Побудова аналітичного запиту**
**Ситуація:** У вас є SQL-скрипт (побудований в AWS Athena) для Tableau-звіту, який збирає дані за останні 2 роки. Скрипт доволі важкий, а його виконання займає значний час.
**Проблема:** Відділ боротьби з шахрайством (Antifraud) просить додати нове поле для визначення гравців з "Risk Status". Список цих гравців (приблизно 30 000 user_id) надається в CSV-файлі.
**Завдання:** Запропонуйте та обґрунтуйте ефективне рішення, яке мінімізує вплив на продуктивність запиту. Надайте оновлений SQL-скрипт, який включає нове поле.

**Оригінальний запит:**
SQL
```sql
SELECT
    user_id,
    as_of_date,
    SUM(deposit_amount) AS deposit_amount,
    SUM(sport_ggr) AS sport_ggr,
    ... -- Інші метрики
    SUM(turnover) AS turnover,
    SUM(ggr) AS ggr,
FROM
    database.my_table
WHERE
    as_of_date >= DATE '2023-01-01'
GROUP BY
    user_id
```

##### 🔹 Рекомендоване рішення
**1. Завантажити CSV зі списком risk user_id у окрему таблицю в S3 / Athena (наприклад, `risk_users`)**
- Створити таблицю `risk_users(user_id int)` через Athena **як зовнішню таблицю на CSV у S3**.
- Переваги:
    - Athena може робити **JOIN** замість довгого списку `IN`
    - Оптимізовано для великої кількості user_id
    - Просте оновлення списку Risk User (оновлюємо CSV)

**2. Використати LEFT JOIN та CASE**
- Додаємо поле `risk_status`, яке позначає, чи користувач у списку Risk User.

```sql
WITH risk AS (
    SELECT user_id
    FROM database.risk_users
)
SELECT
    t.user_id,
    t.as_of_date,
    SUM(t.deposit_amount) AS deposit_amount,
    SUM(t.sport_ggr) AS sport_ggr,
    -- інші метрики
    SUM(t.turnover) AS turnover,
    SUM(t.ggr) AS ggr,
    CASE
        WHEN r.user_id IS NOT NULL THEN 'High Risk'
        ELSE 'Normal'
    END AS risk_status
FROM
    database.my_table t
LEFT JOIN
    risk r
ON
    t.user_id = r.user_id
WHERE
    t.as_of_date >= DATE '2023-01-01'
GROUP BY
    t.user_id,
    t.as_of_date,
    CASE
        WHEN r.user_id IS NOT NULL THEN 'High Risk'
        ELSE 'Normal'
    END
```


#### **2.3. Останній логін та девайс**
Напишіть запит, який для кожного користувача виведе **дату останнього логіну** та **девайс**, з якого він був зроблений.
**Таблиця:**
· user_logins: user_id (int), login_date (timestamp), client_device (string)

**==Рішення:==**
```sql 
WITH last_login AS (
    SELECT
        user_id,
        MAX(login_date) AS last_login_date
    FROM user_logins
    GROUP BY user_id
)
SELECT
    l.user_id,
    l.last_login_date,
    u.client_device
FROM last_login l
JOIN user_logins u
    ON l.user_id = u.user_id
   AND l.last_login_date = u.login_date
ORDER BY l.user_id;
```

Пояснення
- **`MAX(login_date)` у CTE `last_login`**
    - Знаходимо останню дату логіну для кожного користувача.
- **JOIN з оригінальною таблицею `user_logins`**
    - Повертаємо всі записи, де `login_date = last_login_date`.
    - Якщо користувач логінився з декількох девайсів одночасно → всі ці записи будуть у результаті.
- **ORDER BY user_id**
    - Для зручності перегляду групуємо по юзер айді


