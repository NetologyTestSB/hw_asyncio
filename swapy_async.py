import asyncio
import datetime
import aiohttp
from more_itertools import chunked

from models import Base, SwapiPeople, engine, Session

# import requests

MAX_REQUESTS_CHUNK = 5


async def get_data(url, key, session):
    response = await session.get(url)
    data = await response.json()
    return data[key], response.status


async def get_data_str(link_list, key, session):
    if link_list is None:
        return []
    data_list = []
    for link in link_list:
        data, status = await get_data(link, key, session)
        if status != 200:
            return 'status error'
        data_list.append(data)
    data_str = ', '.join(data_list)
    return data_str


async def prepare_person_model(person):
    session = aiohttp.ClientSession()
    films_str = await get_data_str(person['films'], 'title', session)
    species_str = await get_data_str(person['species'], 'name', session)
    starships_str = await get_data_str(person['starships'], 'name', session)
    vehicles_str = await get_data_str(person['vehicles'], 'name', session)
    await session.close()

    person_model = SwapiPeople(
            birth_year=person['birth_year'],
            eye_color=person['eye_color'],
            gender=person['gender'],
            hair_color=person['hair_color'],
            height=person['height'],
            mass=person['mass'],
            name=person['name'],
            skin_color=person['skin_color'],
            homeworld=person['homeworld'],
            films=films_str,
            species=species_str,
            starships=starships_str,
            vehicles=vehicles_str
        )
    return person_model


async def insert_people(people_list_json):
    # async with Session() as session:
    people_list = []
    for person in people_list_json:
        if person.get('name'):
            people_list.append(await prepare_person_model(person))

    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_person(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    for person_ids_chunk in chunked(range(1, 200), MAX_REQUESTS_CHUNK):
        person_coros = [get_person(person_id) for person_id in person_ids_chunk]
        people = await asyncio.gather(*person_coros)

        insert_people_coro = insert_people(people)
        asyncio.create_task(insert_people_coro)

    main_task = asyncio.current_task()
    insert_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insert_tasks)

    print('done')

if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)