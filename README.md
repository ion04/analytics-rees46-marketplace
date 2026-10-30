# Прогноз покупки в сессии и триггерные коммуникации для eCommerce

Учебный проект по продуктовой аналитике (МГТУ им. Н.Э. Баумана, осень 2026), трек Б.
Сервис оценивает вероятность покупки в текущей сессии по цепочке `view → cart → purchase` и запускает
напоминание о корзине или скидку только в зоне средней вероятности. Подробное описание продукта,
данных, определений, метрик, трекинга, расчётов, этики и критериев успеха — в ноутбуке [`intro.ipynb`](intro.ipynb).

## Данные

- **Источник:** REES46, [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Kaggle, Open CDP).
- **Период:** ноябрь 2019 – февраль 2020 (4 месячных CSV), 246 330 463 события, 10 723 863 пользователя.
- **Лицензия:** `Data files © Original Authors`; обязательно указывать источник. Из-за лицензии и объёма
  **данные в репозиторий не включены**.
- Каналы привлечения и расходы на маркетинг в датасете отсутствуют. Для LTV/CAC они **синтетические**
  (`pipeline.py`, `SEED` из `.env`); все выводы по LTV/CAC сценарные.

## Как запустить

Нужны Python 3.13, Docker и токен Kaggle (`~/.kaggle/kaggle.json`).

```bash
# 1. Окружение
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Настройки: скопировать шаблон и задать пароли
cp .env.example .env

# 3. Данные: только четыре нужных месяца
mkdir -p data
for f in 2019-Nov 2019-Dec 2020-Jan 2020-Feb; do
  kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store -f $f.csv -p data/ --unzip
done

# 4. ClickHouse (и Superset)
docker compose up -d

# 5. Загрузка и витрины: tables, load, channels, spend, cohorts, views
python pipeline.py
```

Затем откройте `intro.ipynb` и выполните ячейки сверху вниз. Для ClickHouse нужно не менее 8 ГБ памяти
(`mem_limit` в `docker-compose.yaml`).

Отдельные шаги: `python pipeline.py tables load` (список шагов — в начале `pipeline.py`).

## Что получилось

- **Качество данных:** дубли ≈ 0,43 %; пропуски `brand` ≈ 13,2 %, `category_code` ≈ 15,8 %;
  6 171 аномально активный пользователь (> 1000 событий) исключён из когортного анализа.
- **Конверсия:** `CR cart→purchase` = 44,64 %, `CR session→purchase` = 5,66 %.
- **Retention** (активность в следующем месяце после привлечения): 28,1 % (декабрьская когорта), 23,8 % (январская);
  покупка в следующем месяце ≈ 2,5 % когорты. Ноябрьская когорта завышена вернувшимися клиентами.
- **LTV/CAC по каналам** (синтетические расходы): email 12,24; organic_search 6,92; referral 5,61;
  paid_search 1,25; social 0,75.

## Структура

| Файл | Назначение |
|:--|:--|
| `intro.ipynb` | Основной ноутбук: разделы 1–9 |
| `pipeline.py` | Загрузка CSV в ClickHouse, синтетические каналы и расходы, когорты, витрины |
| `docker-compose.yaml` | ClickHouse, Superset (Postgres, Redis) |
| `.env.example` | Шаблон настроек (секреты хранятся в `.env`, он в Git не попадает) |
| `requirements.txt` | Зависимости Python |

## Этика

Данные обезличены и не передаются третьим лицам; хранятся локально и исключены из Git. Подробности — в разделе 7 ноутбука.
