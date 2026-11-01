# B2B-платформа продуктовой аналитики и ETL-пайплайн для eCommerce

Сервис собирает сырые логи веб-аналитики, очищает их от технического шума и в автоматическом режиме строит витрины данных для расчета метрик юнит-экономики , когортного удержания и воронки продаж. 

В проекте реализована **трёхуровневая медальная архитектура (Medallion Architecture)** на базе СУБД ClickHouse. Визуализация данных для бизнеса выполняется в Apache Superset. Подробное описание продукта, данных, определений, метрик, трекинга, расчётов и этики — в ноутбуке [`intro.ipynb`](intro.ipynb).

##  Архитектура данных
*   **Bronze (`shop.events_raw`)** — сырые события из CSV без изменений (Append-Only).
*   **Silver (`shop.events`, `products_dim`, `excluded_users`)** — очищенные данные. Отсеян технический шум, удалены дубликаты (через `ReplacingMergeTree`), заменены `NULL`, аномальные пользователи  вынесены в отдельный блэклист.
*   **Gold (`shop.cohort_retention`, `shop.user_channels`, `shop.channel_spend`)** — агрегированные витрины, готовые для построения дашбордов и расчета продуктовых метрик.

##  Данные

-   **Источник:** REES46, [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Kaggle, Open CDP).
-   **Период:** ноябрь 2019 – февраль 2020 (4 месячных CSV).
-   **Объем:** 246 330 463 события, 10 723 863 уникальных пользователя.
-   **Лицензия:** `Data files © Original Authors`; обязательно указывать источник. Из-за лицензии и объёма **данные в репозиторий не включены**.
-   **Синтетика:** Каналы привлечения и маркетинговые расходы в датасете отсутствуют. Для расчета LTV/CAC они генерируются синтетически (`pipeline.py`, `SEED=26`). Все выводы по экономике каналов носят сценарный характер.

##  Как запустить

Нужны: Python 3.11+, Docker и токен Kaggle (`~/.kaggle/kaggle.json`).

```bash
# 1. Окружение
python -m venv .venv
source .venv/bin/activate 
pip install -r requirements.txt

# 2. Настройки: скопировать шаблон и задать пароли
cp .env.example .env

# 3. Данные: скачиваем только четыре нужных месяца
mkdir -p data
for f in 2019-Nov 2019-Dec 2020-Jan 2020-Feb; do
  kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store -f $f.csv -p data/ --unzip
done

# 4. Инфраструктура: ClickHouse и Apache Superset
docker compose up -d

# 5. Запуск ETL-пайплайна: создание таблиц, загрузка, очистка, витрины
python pipeline.py