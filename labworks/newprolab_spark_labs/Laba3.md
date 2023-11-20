# Лаба 3. Создание витрины данных из разных источников: файлы, NoSQL-хранилища, реляционные базы данных

## О витринах

_Витрина данных (англ. Data Mart; другие варианты перевода: хранилище данных специализированное, киоск данных, рынок данных) — срез хранилища данных, представляющий собой массив тематической, узконаправленной информации, ориентированный, например, на пользователей одной рабочей группы или департамента._ (Wikipedia)

Лабораторная предназначена для реализации на Dataframe API.


## I. С высоты птичьего полета

У вас есть наборы данных в разных источниках:

* Cassandra – информация о клиентах: uid, пол и возраст.
* Elasticsearch – логи посещения интернет-магазина из экосистемы некоего технологического гиганта: страницы товаров, которые открывали посетители магазина, чтобы посмотреть товар или купить его.
* HDFS – информация о посещениях сторонних веб-сайтов пользователями, приобретенная у вендора: uid, набор (url, timestamp). 
* PostgreSQL - информация о тематических категориях веб-сайтов, приобретенная у вендора.

Вам надо агрегировать данные из этих источников в витрину на основе PostgreSQL для отдела маркетинга, чтобы они могли делать свой анализ и таргетировать предложения для клиентов на основе их предпочтений в сети и в магазине.

![Alt text](/images/img3.png?raw=true "Архитектура")

Адреса Cassandra, Elasticsearch, PostgreSQL находятся [здесь](../tech_info.md)

Для сохранения в PostgreSQL используйте персональную базу имя_фамилия, ваш логин имя_фамилия и пароль из личного кабинета.

## II. Описание данных

### a. Информация о клиентах

Информация о клиентах хранится в Cassandra (keyspace `labdata`, table `clients`) в следующем виде:

* `uid` – уникальный идентификатор пользователя, string
* `gender` – пол пользователя, F или M - string
* `age` – возраст пользователя в годах, integer

### b. Логи посещения интернет-магазина

*Elasticsearch index: visits*

Из бэкенда интернет-магазина приходят отфильтрованные и обогащенные сообщения о просмотрах страниц товаров и покупках. Сообщения хранятся в Elasticsearch в формате json в следующем виде:

* `uid` – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra), либо null, если в базе пользователей нет информации об этих посетителях магазина, string
* `event_type` – buy или view, соответственно покупка или просмотр товара, string
* `category` – категория товаров в магазине, string
* `item_id` – идентификатор товара, состоящий из категории и номера товара в категории, string
* `item_price` – цена товара, integer
* `timestamp` – unix epoch timestamp в миллисекундах

### c. Логи посещения веб-сайтов

`hdfs:///labs/laba03/weblogs.json`

Логи хранятся в формате json на HDFS и имеют следующую структуру:

* `uid` – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra),
* массив `visits` c некоторым числом пар (timestamp, url), где `timestamp` – unix epoch timestamp в миллисекундах, `url` - строка.

В этом датасете не все записи содержат `uid`. Это означает, что были покупатели, еще пока не идентифицированные и не внесенные в базу данных клиентов. Покупки и просмотры таких покупателей можно игнорировать в этом задании.

### d. Информация о категориях веб-сайтов

Эта информация хранится в базе данных PostgreSQL `labdata` таблице `domain_cats`:

* `domain` (только второго уровня), string
* `category`, string 

Используйте ваш логин (нужно заменить в логине "." на "_") и пароль от ЛК для соединения с PostgreSQL.

## III. Задание 

Ознакомьтесь с памяткой по [PostgreSQL](../PostgreSQL.md).

Используя psql, создайте в вашей базе данных `имя_фамилия` таблицу `clients`  со следующими колонками:

uid, gender, age_cat, shop_cat1, ... , shop_catN, web_cat1, ... , web_catN

где:
* `uid` (primary key) – uid пользователя.
* `gender` – пол пользователя: `M`, `F`.
* `age_cat` – категория возраста, одна из пяти: `18-24`, `25-34`, `35-44`, `45-54`, `>=55`.
* `shop_cat`, `web_cat` – категории товаров и категории веб-сайтов.

Дайте пользователю `labchecker2` привилегию на `SELECT` из этой таблицы.
> При использовании overwrite при записи в таблицу Postgres, нужно давать гранты на чтение таблицы каждый раз после пересоздания таблицы.

Внимание! 

* Категории товаров берутся из логов посещения интернет-магазина. Чтобы сделать из категорий названия колонок, обратите внимание на метод pivot. Названия категорий приводятся к нижнему регистру, пробелы или тире заменяются на подчеркивание, к категории прибавляется приставка `shop_`. Например  `shop_everyday_jewelry`.

* Категории веб-сайтов берутся из датасета категорий вебсайтов, и точно также из них создаются имена колонок. Например: `web_arts_and_entertainment`.
* После вычленения домена из URL нужно удалить из доменов "www."

В колонках категорий товаров должно быть число посещений соответствующих страниц, а в колонках категорий веб-сайтов - число посещений соответствующих веб-сайтов.

Как оформить лабораторную смотрите ниже.

В случае проблем смотрите [Полезные советы](../FAQ.md), пункт Решение проблем.

## IV. Оформление работы

Начиная с этой лабораторной и для всех последующих лабораторных, выкладывать их в репозиторий курса и делать pull-request больше не надо.

Оформите решение в виде [Spark проекта](../idea.md).

Начиная с этой лабораторной и для всех последующих лабораторных, выкладывайте проект для проверки чекером в новый репозиторий: [Работа с приватным Git репозиторием](../Github_Repo.md).

Проект должен компилироваться и запускаться следующим образом:

```
cd lab03/data_mart
sbt package
spark-submit --packages org.elasticsearch:elasticsearch-spark-20_2.11:версия_Elastichsearch,com.datastax.spark:spark-cassandra-connector_2.11:версия_Cassandra,org.postgresql:postgresql:версия_библиотеки_PostgreSQL --class data_mart target/scala-2.11/data_mart_2.11-1.0.jar 
```

Версии можно узнать [здесь](../tech_info.md).

В случае проблем смотрите [Полезные советы](../FAQ.md), пункт Решение проблем.

## V. Проверка

Чекер проверит:
* наличие файла с решением в репо, наличие .gitignore и его содержимое,
* наличие и структуру таблицы,
* число строк (пользователей) в таблице,
* число пользователей в каждой возрастной категории,
* некоторые записи, сравнив их с эталонными.

В случае проблем смотрите [Полезные советы](../FAQ.md), пункт Решение проблем.

### Поля чекера

* `git_correct` = Тrue/False: в репо sb-spark существует `lab03/data_mart/data_mart.scala`.
* `info_git_errors` = "": ошибки при работе с репозиторием, если есть.
* `data_mart_table_correct` = Тrue/False: структура таблицы правильная, совпадают все колонки.
* `data_mart_number_of_users_correct` = Тrue/False: правильное число пользователей в таблице.
* `data_mart_gender_age_cat_correct` = Тrue/False: правильное распределение пользователей по половозрастным категориям.
* `data_mart_user1_correct` = Тrue/False: для некоторого пользователя правильно указаны значения в колонках категорий.
* `info_data_mart_errors` = "": ошибки при работе с базой данных.
* `info_your_data` = {}: данные, полученные чекером через запросы в ваши таблицы.
* `lab_result` = True/False: зачет или нет (необходимо на всех проверяемых пунктах получить True).

## Подсказки

Как прописать зависимости в build.sbt:
```
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "версия_Cassandra"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "версия_Elastichsearch"
libraryDependencies += "org.postgresql" % "postgresql" % "версия_библиотеки_PostgreSQL"
```

Версии можно узнать [здесь](../tech_info.md).

Как прописать зависимости в Jupyter notebook:
```
%AddDeps com.datastax.spark spark-cassandra-connector_2.11 версия_Cassandra
%AddDeps org.elasticsearch elasticsearch-spark-20_2.11 версия_Elastichsearch
%AddDeps org.postgresql postgresql версия_библиотеки_PostgreSQL
```

Пример чтения из Cassandra:
```
import org.apache.spark.sql.{DataFrame, SparkSession}

//...
//внутри класса/объекта:

  val spark: SparkSession = SparkSession.builder()
    .config("spark.cassandra.connection.host", "адрес_Cassandra")
    .config("spark.cassandra.connection.port", "порт_Cassandra")
    .getOrCreate()
    
  val clients: DataFrame = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "таблица_Cassandra", "keyspace" -> "кейспейс_Cassandra"))
    .load()
```

Внимание! Никогда не используйте .master("yarn") при создании сессии в коде!

Пример чтения из Elasticsearch:
```
import org.apache.spark.sql.DataFrame

//...
//внутри класса/объекта:

  val visits: DataFrame = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(Map("es.read.metadata" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.port" -> "порт_Elasticsearch",
      "es.nodes" -> "адрес_Elasticsearch",
      "es.net.ssl" -> "false"))
    .load("индекс_Elasticsearch")
```

Пример чтения из HDFS:
```
import org.apache.spark.sql.DataFrame

//...
//внутри класса/объекта:

  val logs: DataFrame = spark.read
    .json("hdfs:///labs/laba03/weblogs.json")
```

Пример декодирования URL и извлечения домена с помощью UDF:
```
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.util.Try

//...
//внутри класса/объекта:

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
  })
  
  transformedLogs.select(col("uid"), decodeUrlAndGetDomain(col("url")).alias("domain"))
```
Не забывайте, что после выделения домена возможно вам придется отфильтровать пустые домены.

Пример чтения из Postgres:
```
import org.apache.spark.sql.DataFrame

//...
//внутри класса/объекта:

  val cats: DataFrame = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://адрес_Postgres:порт_Postgres/входная_база_Postgres")
    .option("dbtable", "входная_таблица_Postgres")
    .option("user", "имя_фамилия")
    .option("password", "********")
    .option("driver", "org.postgresql.Driver")
    .load()
```

Запись в Postgres:
```
//...
//внутри класса/объекта:

  result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://адрес_Postgres:порт_Postgres/выходная_база_Postgres") //как в логине к личному кабинету но _ вместо .
    .option("dbtable", "выходная_таблица_Postgres")
    .option("user", "имя_фамилия")
    .option("password", "*** *****")
    .option("driver", "org.postgresql.Driver")
    .option("truncate", value = true) //позволит не терять гранты на таблицу
    .mode("overwrite") //очищает данные в таблице перед записью
    .save()
```

Дать права на таблицу в Postgres программно (для удобства), но можно и один раз руками выдать:
```
import org.postgresql.Driver
import java.sql.{Connection, DriverManager, Statement}

//...
//внутри класса/объекта:

  //вызвать после сохранения в Postgres: grantTable()
  def grantTable(): Unit = {
    val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
    val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
    val url = "jdbc:postgresql://адрес_Postgres:порт_Postgres/выходная_база_Postgres?user=имя_фамилия&password=********"
    val connection: Connection = DriverManager.getConnection(url)
    val statement: Statement = connection.createStatement()
    val bool: Boolean = statement.execute("GRANT SELECT ON выходная_таблица_Postgres TO пользователь_чекера")
    connection.close()
  }
```

Где искать документацию Спарка (на лекциях показывают; замените версию Spark на [нужную](../tech_info.md)):

В частности функции Спарка здесь https://spark.apache.org/docs/2.4.7/api/scala/index.html#org.apache.spark.sql.functions$ \
SQL функции https://spark.apache.org/docs/2.4.7/api/sql/index.html \
Методы Dataset https://spark.apache.org/docs/2.4.7/api/scala/index.html#org.apache.spark.sql.Dataset \
Методы Column https://spark.apache.org/docs/2.4.7/api/scala/index.html#org.apache.spark.sql.Column \
Методы RelationalGroupedDataset https://spark.apache.org/docs/2.4.7/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset

### Самопроверка

- Убедитесь, что после чтения из Cassandra и добавления категорий возраста у вас такое кол-во записей по полу и возрасту (см. таблицу ниже).\
Такое же количество будет в выходном датасете.

|gender|age_cat|count(1)|
|------|-------|--------|
|     F|  18-24|    2886|
|     F|  25-34|    6791|
|     F|  35-44|    4271|
|     F|  45-54|    2597|
|     F|   >=55|     895|
|     M|  18-24|    2012|
|     M|  25-34|    8666|
|     M|  35-44|    5089|
|     M|  45-54|    2147|
|     M|   >=55|     784|

- Убедитесь, что из Cassandra вы прочитали 36138 записей. Такое же количество должно быть в выходном датасете.
- Чекер считает те записи, которые считает правильными, поэтому если чекер вам показывает другие каунты - значит ошибки в данных.
- Пример правильного ответа чекера:
```
00_git_correct: True
01_info_git_errors:
02_data_mart_table_correct: True
03_data_mart_number_of_users_correct: True
04_data_mart_gender_age_cat_correct: True
05_data_mart_user1_correct: True
06_info_postgres_errors:
07_info_your_data: {'Your tables': ['clients'], 'Your columns': ['uid', 'age_cat', 'gender', 'shop_cameras', 'shop_clothing', 'shop_computers', 'shop_cosmetics', 'shop_entertainment_equipment', 'shop_everyday_jewelry', 'shop_house_repairs_paint_tools', 'shop_household_appliances', 'shop_household_furniture', 'shop_kitchen_appliances', 'shop_kitchen_utensils', 'shop_luggage', 'shop_mobile_phones', 'shop_shoes', 'shop_sports_equipment', 'shop_toys', 'web_arts_and_entertainment', 'web_autos_and_vehicles', 'web_beauty_and_fitness', 'web_books_and_literature', 'web_business_and_industry', 'web_career_and_education', 'web_computer_and_electronics', 'web_finance', 'web_food_and_drink', 'web_gambling', 'web_games', 'web_health', 'web_home_and_garden', 'web_internet_and_telecom', 'web_law_and_government', 'web_news_and_media', 'web_pets_and_animals', 'web_recreation_and_hobbies', 'web_reference', 'web_science', 'web_shopping', 'web_sports', 'web_travel'], 'Number of records returned': [(36138,)], 'Gender_age numbers returned': [('F', '35-44', 4271), ('F', '25-34', 6791), ('M', '18-24', 2012), ('F', '18-24', 2886), ('M', '35-44', 5089), ('F', '45-54', 2597), ('M', '45-54', 2147), ('M', '25-34', 8666), ('F', '>=55', 895), ('M', '>=55', 784)], 'USER1': {'uid': 'd50192e5-c44e-4ae8-ae7a-7cfe67c8b777', 'age_cat': '18-24', 'gender': 'F', 'shop_computers': 2, 'shop_entertainment_equipment': 1, 'shop_mobile_phones': 1, 'web_news_and_media': 4}}
08_lab_result: True
status: last check failed
```
- В п. 6 чекер в случае ошибок показывает каунты, которые должны быть, например:
```
06_info_postgres_errors: Number of users in the table is not correct. These gender cat records are incorrect: {('M', '35-44', 5089), ('M', '25-34', 8666)}
```
Ваши каунты вы можете посмотреть в вашей таблице.
- Обратите внимание, что чекер показывает вам ваши таблицы - вы можете смотреть в одну, тогда как чекер смотрит в другую - будьте внимательны. Например:
```'Your tables': ['client', 'clients']```

В случае проблем смотрите [Полезные советы](../FAQ.md), пункт Решение проблем.
