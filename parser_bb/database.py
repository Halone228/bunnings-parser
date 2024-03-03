from functools import lru_cache

from sqlalchemy import create_engine, update, bindparam, select, func
from sqlalchemy.orm import declarative_base, DeclarativeBase, sessionmaker, mapped_column, Mapped
from sqlalchemy.dialects.sqlite import insert
from os import getenv
from loguru import logger

USER = getenv('POSTGRES_USER', 'default')
PASSWORD = getenv('POSTGRES_PASSWORD', 'pass')
HOST = getenv('POSTGRES_HOST', '127.0.0.1')
PORT = getenv('POSTGRES_PORT', 5432)
DB = getenv('POSTGRES_DB', 'db')

uri = f'{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}'

base: DeclarativeBase = declarative_base()

engine = create_engine(f'postgresql+psycopg2://{uri}', pool_size=20)
sesmaker = sessionmaker(bind=engine)


class ProductModel(base):
    __tablename__ = 'product_table'
    article: Mapped[str] = mapped_column(
        primary_key=True
    )
    url: Mapped[str]
    count: Mapped[int] = mapped_column(
        default=0
    )
    images: Mapped[str]
    breadcrumbs: Mapped[str]
    price: Mapped[str]
    description: Mapped[str]


with engine.connect() as connection:
    base.metadata.create_all(connection)
    connection.commit()
engine.dispose()

@logger.catch()
def insert_data(
    data: list[dict]
):
    with sesmaker() as session:
        session.execute(
            insert(ProductModel).on_conflict_do_nothing().execution_options(synchronize_session=False),
            data
        )
        session.commit()


def update_stock(
    data: list[dict]
):
    with sesmaker() as session:
        session.execute(
            update(ProductModel).execution_options(synchronize_session=False),
            data
        )
        session.commit()


def update_data(
    data: list[dict]
):
    with sesmaker() as session:
        session.execute(
            update(ProductModel).where(
                url=bindparam('url')
            ).values(
                description=bindparam('description'),
                breadcrumbs=bindparam('breadcrumbs'),
                images=bindparam('images')
            ).execution_options(synchronize_session=False),
            data
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
