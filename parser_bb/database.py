from functools import lru_cache

from sqlalchemy import create_engine, update, bindparam, select, func, ForeignKey
from sqlalchemy.orm import (
    declarative_base,
    DeclarativeBase,
    sessionmaker,
    mapped_column,
    Mapped,
)
from sqlalchemy.dialects.sqlite import insert
from os import getenv
from loguru import logger

USER = getenv("POSTGRES_USER", "default")
PASSWORD = getenv("POSTGRES_PASSWORD", "pass")
HOST = getenv("POSTGRES_HOST", "127.0.0.1")
PORT = getenv("POSTGRES_PORT", 5432)
DB = getenv("POSTGRES_DB", "db")

uri = f"{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

base: DeclarativeBase = declarative_base()

engine = create_engine(f"postgresql+psycopg2://{uri}", pool_size=20)
sesmaker = sessionmaker(bind=engine)


class ProductModel(base):
    __tablename__ = "product_table"
    article: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(
        nullable=True
    )
    url: Mapped[str]
    count: Mapped[int] = mapped_column(default=0)
    images: Mapped[str]
    breadcrumbs: Mapped[str]
    price: Mapped[str]
    description: Mapped[str]


class StockParsed(base):
    __tablename__ = "stock_parsed_table"
    article_id: Mapped[str] = mapped_column(
        ForeignKey("product_table.article"), primary_key=True
    )
    stock_val: Mapped[int]


class FullInfoParsed(base):
    __tablename__ = "full_info_parsed_table"
    url: Mapped[str] = mapped_column(primary_key=True)
    images: Mapped[str]
    breadcrumbs: Mapped[str]
    description: Mapped[str]


with engine.connect() as connection:
    base.metadata.create_all(connection)
    connection.commit()
engine.dispose()


@logger.catch()
def insert_data(data: list[dict]):
    with sesmaker() as session:
        session.execute(
            insert(ProductModel)
            .on_conflict_do_nothing()
            .execution_options(synchronize_session=False),
            data,
        )
        session.commit()


def update_stock(data: list[dict]):
    with sesmaker() as session:
        session.execute(
            update(ProductModel).execution_options(synchronize_session=False), data
        )
        session.commit()


def update_data(_data: list[dict]):
    data = _data.copy()
    with sesmaker() as session:
        url_to_article = get_urls_articles()

        def artic(item):
            nonlocal url_to_article
            item["article"] = url_to_article[item.pop("url")]
            return item

        session.execute(
            update(ProductModel).execution_options(synchronize_session=False),
            list(map(artic, data)),
        )
        session.commit()


@lru_cache(maxsize=-1)
def get_products_count():
    with sesmaker() as session:
        return session.execute(select(func.count()).select_from(ProductModel)).scalar()


def get_all_articles():
    with sesmaker() as session:
        return session.execute(select(ProductModel.article)).scalars().all()


def get_all_urls():
    with sesmaker() as session:
        return session.execute(select(ProductModel.url)).scalars().all()


@lru_cache(maxsize=-1)
def get_urls_articles():
    with sesmaker() as session:
        data = session.execute(
            select(ProductModel.url, ProductModel.article).select_from(ProductModel)
        ).all()
        return dict(data)


def set_stock_parsed(data: dict[str, int]):
    if not data:
        return
    with sesmaker() as session:
        session.execute(
            insert(StockParsed).on_conflict_do_nothing(),
            [{"article_id": k, "stock_val": v} for k, v in data.items()],
        )
        session.commit()


def set_full_info_parsed(data: list[dict]):
    if not data:
        return
    with sesmaker() as session:
        session.execute(insert(FullInfoParsed).on_conflict_do_nothing(), data)
        session.commit()


def compress_stock_parsed(articles: list[str]):
    with sesmaker() as session:
        result = session.execute(
            select(StockParsed.article_id)
            .select_from(StockParsed)
            .where(StockParsed.article_id.in_(articles))
            .execution_options(stream_results=True)
        )
        return set(articles) - set(result.scalars())


def compress_full_info_parsed(data: list[str]):
    with sesmaker() as session:
        result = session.execute(
            select(FullInfoParsed.url)
            .select_from(FullInfoParsed)
            .where(FullInfoParsed.url.in_(data))
            .execution_options(stream_results=True)
        )
        return set(data) - set(result.scalars())


if __name__ == "__main__":
    get_urls_articles()
