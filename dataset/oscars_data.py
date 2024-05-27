"""Creates a new dataset with more data about the films."""
import requests
import asyncio
import os
from dotenv import load_dotenv
import polars as pl

load_dotenv()

api_key = os.environ["TMDB_API_KEY"]
token = os.environ["TMDB_TOKEN"]

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {token}"
}

def query(name: str) -> str:
    """Function converts query string to a request readable format."""

    film_name = name
    film_split = film_name.split(' ')
    query_string = "%20".join(film_split)

    return query_string

async def film_search(name: str, year: int) -> str:
    """Function send a query request to TMDb and returns film data. It takes in a 'name' and 'year' parameter,
    the 'year' parameter by default is the year the film won an Oscar.
    """
    if name == ' ' or (name is None):
        return 0
    
    
    film_query = query(name)
    film_year = year
    film_url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={film_query}&include_adult=false&language=en-US&page=1&year={film_year}"

    req = requests.get(film_url, timeout=600)
    res = req.json()

    if res["total_results"] != 0:
        tmdb_id = res["results"][0]["id"]
        details_link = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
        req = requests.get(details_link, headers=headers, timeout=600)
        res = req.json()

        return res
    
    film_year -= 1
    new_film_url = f"https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={film_query}&include_adult=false&language=en-US&page=1&year={film_year}"
    new_req = requests.get(new_film_url, timeout=600)
    new_res = new_req.json()

    await asyncio.sleep(1)

    if new_res["total_results"] != 0:
        tmdb_id = new_res["results"][0]["id"]
        details_link = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
        req = requests.get(details_link, headers=headers, timeout=600)
        res = req.json()
        return res
        # return new_res["results"][0]["id"]
    else:
        return None
    
async def main():
    oscars_df = pl.read_csv('./the_oscar_award.csv')

    pl.Config.set_tbl_width_chars = 300

    build_df = oscars_df.select(
        pl.col("film"),
        pl.col("name"),
        pl.col("category"),
        pl.col("ceremony"),
        pl.col("year_ceremony"),
        pl.col("year_film"),
        pl.col("winner")
    )

    build_df = build_df.select(pl.head(["film", "name", "category", "ceremony", "year_ceremony", "year_film", "winner"], 10))

    build_dict = build_df.to_dicts()

    coros = [film_search(film["film"], film["year_ceremony"]) for film in build_dict]
    results = await asyncio.gather(*coros)
    # print(results)

    # Add the tmdb_id to each film dictionary
    for film, tmdb_id in zip(build_dict, results):
        film["adult"] = tmdb_id["adult"]
        film["budget"] = tmdb_id["budget"]
        film["revenue"] = tmdb_id["revenue"]
        film["runtime"] = tmdb_id["runtime"]

    final_df = pl.from_dicts(build_dict)
    # final_df.write_json("tmdb.json", pretty=True)
    with pl.Config(fmt_str_lengths=1000):
        print(final_df.head(10))

if __name__ == "__main__":

    asyncio.run(main())

    # -- TEST CASES --
    # print(asyncio.run(film_search("The Noose", 1928)))
    # print(asyncio.run(film_search("The Dove", 1928)))
    # print("\n")
    # print(film_search("Dodsworth", 1937))
    # print("\n")
    # print(film_search("The Charge of the Light Brigade", 1937))