from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor

# ---------------------------------- auxiliary ----------------------------------
def CreateCriticFromResultSet(result_set: ResultSet) -> Critic:
    new_critic = Critic.badCritic()
    if (result_set is not None) and (len(result_set.rows) != 0):
        new_critic.setCriticID(result_set.rows[0][0])
        new_critic.setName(result_set.rows[0][1])
    return new_critic


def CreateActorFromResultSet(result_set: ResultSet) -> Actor:
    new_actor = Actor.badActor()
    if (result_set is not None) and (len(result_set.rows) != 0):
        new_actor.setActorID(result_set.rows[0][0])
        new_actor.setActorName(result_set.rows[0][1])
        new_actor.setAge(result_set.rows[0][2])
        new_actor.setHeight(result_set.rows[0][3])
    return new_actor


def CreateMovieFromResultSet(result_set: ResultSet) -> Movie:
    new_movie = Movie.badMovie()
    if (result_set is not None) and (len(result_set.rows) != 0):
        new_movie.setMovieName(result_set.rows[0][0])
        new_movie.setYear(result_set.rows[0][1])
        new_movie.setGenre(result_set.rows[0][2])
    return new_movie


def CreateStudioFromResultSet(result_set: ResultSet) -> Studio:
    new_studio = Studio.badStudio()
    if (result_set is not None) and (len(result_set.rows) != 0):
        new_studio.setStudioID(result_set.rows[0][0])
        new_studio.setStudioName(result_set.rows[0][1])
    return new_studio

# ---------------------------------- CRUD API: ----------------------------------

def createTables():
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
                    CREATE TABLE Studios(studio_id INTEGER PRIMARY KEY CHECK (studio_id>0),\
                    studio_name TEXT NOT NULL\
                    );\
                    CREATE TABLE Critics(critic_id INTEGER PRIMARY KEY CHECK (critic_id>0),\
                    critic_name TEXT NOT NULL\
                    );\
                    ---------------relations---------------- \
                    CREATE TABLE PlayedIn(actor_id INTEGER NOT NULL,\
                    movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    salary INTEGER NOT NULL CHECK (salary>0),\
                    num_roles INTEGER NOT NULL CHECK (num_roles>0),\
                    PRIMARY KEY (actor_id,movie_name,year),\
                    FOREIGN KEY (movie_name,year) REFERENCES Movies ON DELETE CASCADE,\
                    FOREIGN KEY (actor_id) REFERENCES Actors ON DELETE CASCADE\
                    );\
                    CREATE TABLE PlayedInRole(actor_id INTEGER NOT NULL,\
                    movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    actor_role TEXT NOT NULL,\
                    PRIMARY KEY (actor_id,movie_name,year,actor_role),\
                    FOREIGN KEY (movie_name,year,actor_id) REFERENCES PlayedIn ON DELETE CASCADE\
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
                    ); \
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
                    DROP TABLE playedin CASCADE;\
                    DROP TABLE playedinrole CASCADE;\
                    DROP TABLE produced CASCADE;\
                    DROP TABLE rated CASCADE;\
                    ")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        critic_id = Critic.getCriticID()
        critic_name = Critic.getName()
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Critics(critic_id, critic_name) VALUES({id}, {name})").format(id=sql.Literal(critic_id), name=sql.Literal(critic_name))
        conn.execute(query)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Critics WHERE id={id}").format(id=sql.Literal(critic_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    res = ReturnValue.OK
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Critics WHERE critic_id={id}").format(id=sql.Literal(critic_id))
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return CreateCriticFromResultSet(result)

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

# -----------------------------------------Basic API--------------------------------------------------------
def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Rated(movie_name,year,critic_id,rating) VALUES({name},{year},{id},{rate})").format(
            name=sql.Literal(movieName), year=sql.Literal(movieYear), id=sql.Literal(criticID), rate=sql.Literal(rating))
        conn.execute(query)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        res = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res



def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Rated WHERE movie_name={name} AND year={y} AND critic_id={id}").format(
            name=sql.Literal(movieName), y=sql.Literal(movieYear), id=sql.Literal(criticID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # TODO: if something goes wrong , suspect this!!!
    conn = None
    res = ReturnValue.OK
    num_roles = len(roles)
    try:
        query = sql.SQL("INSERT INTO PlayedIn(actor_id,movie_name,year,salary,num_roles) Values({id},{name},{year},{s},{n_r});").format(id=sql.Literal(actorID), name=sql.Literal(movieName), year=sql.Literal(movieYear), s=sql.Literal(salary), n_r=sql.Literal(num_roles))

        query += sql.SQL("INSERT INTO PlayedInRole(actor_id,movie_name,year,actor_role) Values")
        for i,r in enumerate(roles):
            query += sql.SQL("({id},{name},{year},{role})").format(id=sql.Literal(actorID), name=sql.Literal(movieName), year=sql.Literal(movieYear), role=sql.Literal(r))
            if i < num_roles-1:
                query += sql.SQL(",")
        query += sql.SQL(";")
        conn.execute(query)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        res = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM PlayedIn WHERE movie_name={name} AND year={y} AND actor_id={id}").format(
            name=sql.Literal(movieName), y=sql.Literal(movieYear), id=sql.Literal(actorID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


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
