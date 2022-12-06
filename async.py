from aiohttp import ClientSession
import asyncio
import datetime
import more_itertools
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Enum

PG_DSN = 'postgresql+asyncpg://app:1234@127.0.0.1:5431/async_hw'
engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(Enum("Male", "Female"))
    hair_color = Column(String)
    height = Column(Integer)
    homeworld = Column(String)
    mass = Column(Integer)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


CHUNK_SIZE = 10


async def foo(tasks_chunk):
    for task in tasks_chunk:
        result = await task
        yield result


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def get_people(people_id, session):
    '''ключевое слово async для выполнения асинхронной функции и слово await для того чтобы функция выполнилась'''
    async with session.get(f'https://swapi.tech/api/people/{people_id}') as response:
        return await response.json()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metedata.create_all)
        await conn.commit()

    async with ClientSession() as http_session:
        tasks = (asyncio.create_task(get_people(i, http_session)) for i in range(1, 101))
        for tasks_chunk in more_itertools.chunked(tasks, CHUNK_SIZE):
            async with Session() as db_session:
                async for result in foo(tasks_chunk):
                    db_session.add(People(json=result))
                await db_session.commit()


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
