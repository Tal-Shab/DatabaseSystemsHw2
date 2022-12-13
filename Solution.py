from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    # TODO: see what exceptions are needed
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("---------------basic tables----------------- \
                    CREATE TABLE Actors(actor_id INTEGER PRIMARY KEY CHECK (actor_id>0),\
                    actor_name TEXT NOT NULL,\
                    age INTEGER NOT NULL CHECK (age>0),\
                    height INTEGER NOT NULL CHECK (height>0)\
                    );\
                    CREATE TABLE Movies(movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL CHECK(year>=1895),\
                    genre TEXT NOT NULL CHECK(genre in ('Drama','Action','Comedy','Horror')),\
                    PRIMARY KEY(movie_name,year)\
                    );\
                    CREATE TABLE Studios(studio_id INTEGER PRIMARY KEY,\
                    studio_name TEXT NOT NULL\
                    );\
                    CREATE TABLE Critics(critic_id INTEGER PRIMARY KEY,\
                    critic_name TEXT NOT NULL\
                    );\
                    ---------------relations---------------- \
                    CREATE TABLE PlayedIn(actor_id INTEGER NOT NULL,\
                    movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    salary INTEGER NOT NULL CHECK (salary>0),\
                    num_roles INTEGER NOT NULL CHECK (num_roles>0),\
                    PRIMARY KEY (actor_id,movie_name,year)\
                    FOREIGN KEY (movie_name,year) REFERENCES Movies ON DELETE CASCADE\
                    FOREIGN KEY (actor_id) REFERENCES Actors ON DELETE CASCADE\
                    );\
                    CREATE TABLE Produced(movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    studio_id INTEGER NOT NULL,\
                    budget INTEGER NOT NULL CHECK(budget>0),\
                    revenue INTEGER NOT NULL CHECK(revenue>0),\
                    PRIMARY KEY(movie_name,year,studio_id),\
                    FOREIGN KEY(movie_name,year) REFERENCES Movies ON DELETE CASCADE,\
                    FOREIGN KEY(studio_id) REFERENCES Studios ON DELETE CASCADE\
                    );\
                    CREATE TABLE Rated(movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    critic_id INTEGER NOT NULL,\
                    rating INTEGER NOT NULL CHECK(rating>=1 AND rating<=5),\
                    PRIMARY KEY(movie_name,year,critic_id),\
                    FOREIGN KEY(movie_name,year) REFERENCES Movies ON DELETE CASCADE,\
                    FOREIGN KEY(critic_id) REFERENCES Critics ON DELETE CASCADE\
                    );\
                    ---------------views---------------- \
                    CREATE VIEW movie_AVG_rating AS\
                    SELECT movie_name,year,AVG(rating) average\
                    FROM Rated\
                    GROUP BY movie_name,year\
                    ;\
                    CREATE VIEW actor_movie_AVG_rating AS\
                    SELECT DISTINCT actor_id, r.movie_name, r.year, average\
                    FROM PlayedIn p RIGHT JOIN movie_AVG_rating r ON p.movie_name=r.movie_name and p.year=r.year\
                    ")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        # i think only this is relevant
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Actors;\
                    DELETE FROM Movies;\
                    DELETE FROM Critics;\
                    DELETE FROM Studios;\
                    ")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        # i think only this is relevant
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def dropTables():
    # TODO: just did it to check git - need to check if this is correct
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE Actors CASCADE;\
                    DROP TABLE Movies CASCADE;\
                    DROP TABLE Critics CASCADE;\
                    DROP TABLE Studios CASCADE;\
                    ")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        # i think only this is relevant
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    # TODO: implement
    pass


def deleteCritic(critic_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getCriticProfile(critic_id: int) -> Critic:
    # TODO: implement
    pass


def addActor(actor: Actor) -> ReturnValue:
    # TODO: implement
    pass


def deleteActor(actor_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getActorProfile(actor_id: int) -> Actor:
    # TODO: implement
    pass


def addMovie(movie: Movie) -> ReturnValue:
    # TODO: implement
    pass


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    # TODO: implement
    pass


def getMovieProfile(movie_name: str, year: int) -> Movie:
    # TODO: implement
    pass


def addStudio(studio: Studio) -> ReturnValue:
    # TODO: implement
    pass


def deleteStudio(studio_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getStudioProfile(studio_id: int) -> Studio:
    # TODO: implement
    pass


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    # TODO: implement
    pass


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    # TODO: implement
    pass


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # TODO: implement
    pass


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    # TODO: implement
    pass


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def studioRevenueByYear() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass

# GOOD LUCK!
