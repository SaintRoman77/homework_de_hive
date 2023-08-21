# homework_de_hive

### Описание:

Практика HIVE. Трансформирование исходных данных, загрузка их на HDFS, выполнение запросов 
по заданию (описание задач ниже).

### Описание задач:

```
1. Трансформировтаь исходные csv-файлы. Добавьить в каждый файл столбец с номером группы таким образом, 
чтобы файл был разделен на 10 групп. В файл customers.csv добавить столбец с номером года, 
в который была совершена подписка (Subscription Date). (см. transform.py)
```

```
2. Загрузить полученные файлы на HDFS. 
(Выполнение определенных команд см. репозиторий introduction_to_hadoop)
```

```
3. СФормировать сводную статистику на уровне каждой компании и на уровне 
каждого года получить целевую возрастную группу подписчиков — то есть, возрастную группу, 
представители которой чаще всего совершали подписку именно в текущий год на текущую компанию.
(см. scr.sql)
```
