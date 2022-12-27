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
def CreateCriticFromResultSet(result_set: ResultSet, rows_affected: int) -> Critic:
    new_critic = Critic.badCritic()
    if (result_set is not None) and (rows_affected > 0):
        new_critic.setCriticID(result_set.rows[0][0])
        new_critic.setName(result_set.rows[0][1])
    return new_critic


def CreateActorFromResultSet(result_set: ResultSet, rows_affected: int) -> Actor:
    new_actor = Actor.badActor()
    if (result_set is not None) and (rows_affected > 0):
        new_actor.setActorID(result_set.rows[0][0])
        new_actor.setActorName(result_set.rows[0][1])
        new_actor.setAge(result_set.rows[0][2])
        new_actor.setHeight(result_set.rows[0][3])
    return new_actor


def CreateMovieFromResultSet(result_set: ResultSet, rows_affected: int) -> Movie:
    new_movie = Movie.badMovie()
    if (result_set is not None) and (rows_affected > 0):
        new_movie.setMovieName(result_set.rows[0][0])
        new_movie.setYear(result_set.rows[0][1])
        new_movie.setGenre(result_set.rows[0][2])
    return new_movie


def CreateStudioFromResultSet(result_set: ResultSet, rows_affected: int) -> Studio:
    new_studio = Studio.badStudio()
    if (result_set is not None) and (rows_affected > 0):
        new_studio.setStudioID(result_set.rows[0][0])
        new_studio.setStudioName(result_set.rows[0][1])
    return new_studio

def ConvertResultSetToList(result_set: ResultSet, rows_affected: int) -> List:
    res = []
    if (result_set is not None) and (rows_affected > 0):
        for i,r in enumerate(result_set.rows):
            res.append(result_set.rows[i][0])
    return res

# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE IF NOT EXISTS Actors(actor_id INTEGER PRIMARY KEY CHECK (actor_id>0),\
                    actor_name TEXT NOT NULL,\
                    age INTEGER NOT NULL CHECK (age>0),\
                    height INTEGER NOT NULL CHECK (height>0)\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS Movies(movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL CHECK(year>=1895),\
                    genre TEXT NOT NULL CHECK(genre in ('Drama','Action','Comedy','Horror')),\
                    PRIMARY KEY(movie_name,year)\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS Studios(studio_id INTEGER PRIMARY KEY CHECK (studio_id>0),\
                    studio_name TEXT NOT NULL\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS Critics(critic_id INTEGER PRIMARY KEY CHECK (critic_id>0),\
                    critic_name TEXT NOT NULL\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS PlayedIn(actor_id INTEGER NOT NULL,\
                    movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    salary INTEGER NOT NULL CHECK (salary>0),\
                    num_roles INTEGER NOT NULL CHECK (num_roles>0),\
                    PRIMARY KEY (actor_id,movie_name,year),\
                    FOREIGN KEY (movie_name,year) REFERENCES Movies ON DELETE CASCADE,\
                    FOREIGN KEY (actor_id) REFERENCES Actors ON DELETE CASCADE\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS PlayedInRole(actor_id INTEGER NOT NULL,\
                    movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    actor_role TEXT NOT NULL,\
                    PRIMARY KEY (actor_id,movie_name,year,actor_role),\
                    FOREIGN KEY (actor_id,movie_name,year) REFERENCES PlayedIn ON DELETE CASCADE\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS Produced(movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    studio_id INTEGER NOT NULL,\
                    budget INTEGER NOT NULL CHECK(budget>=0),\
                    revenue INTEGER NOT NULL CHECK(revenue>=0),\
                    PRIMARY KEY(movie_name,year),\
                    FOREIGN KEY(movie_name,year) REFERENCES Movies ON DELETE CASCADE,\
                    FOREIGN KEY(studio_id) REFERENCES Studios ON DELETE CASCADE\
                    )")
        conn.execute("CREATE TABLE IF NOT EXISTS Rated(movie_name TEXT NOT NULL,\
                    year INTEGER NOT NULL,\
                    critic_id INTEGER NOT NULL,\
                    rating INTEGER NOT NULL CHECK(rating>=1 AND rating<=5),\
                    PRIMARY KEY(movie_name,year,critic_id),\
                    FOREIGN KEY(movie_name,year) REFERENCES Movies ON DELETE CASCADE,\
                    FOREIGN KEY(critic_id) REFERENCES Critics ON DELETE CASCADE\
                    )")
        conn.execute("CREATE VIEW movie_AVG_rating AS\
                    SELECT movie_name,year,AVG(rating) average\
                    FROM Rated\
                    GROUP BY movie_name,year")
        conn.execute("CREATE VIEW actor_movie_AVG_rating AS\
                    SELECT DISTINCT actor_id, p.movie_name, p.year, COALESCE(average,0) as average\
                    FROM PlayedIn p LEFT JOIN movie_AVG_rating r ON p.movie_name=r.movie_name and p.year=r.year")
        conn.execute("CREATE VIEW actor_movie_studio AS\
                    SELECT Distinct actor_id, pl.movie_name, pl.year, pr.studio_id\
                    from playedin pl JOIN produced pr On pl.movie_name=pr.movie_name AND pl.year=pr.year")

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Actors")
        conn.execute("DELETE FROM Movies")
        conn.execute("DELETE FROM Critics")
        conn.execute("DELETE FROM Studios")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Actors CASCADE")
        conn.execute("DROP TABLE IF EXISTS Movies CASCADE")
        conn.execute("DROP TABLE IF EXISTS Critics CASCADE")
        conn.execute("DROP TABLE IF EXISTS Studios CASCADE")
        conn.execute("DROP TABLE IF EXISTS playedin CASCADE")
        conn.execute("DROP TABLE IF EXISTS playedinrole CASCADE")
        conn.execute("DROP TABLE IF EXISTS produced CASCADE")
        conn.execute("DROP TABLE IF EXISTS rated CASCADE")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        critic_id = critic.getCriticID()
        critic_name = critic.getName()
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Critics(critic_id, critic_name) VALUES({id}, {name})").format(
            id=sql.Literal(critic_id), name=sql.Literal(critic_name))
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
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Critics WHERE critic_id={id}").format(id=sql.Literal(critic_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    result = ResultSet()
    rows_in_output = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Critics WHERE critic_id={id}").format(id=sql.Literal(critic_id))
        rows_in_output, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return CreateCriticFromResultSet(result, rows_in_output)


def addActor(actor: Actor) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        actor_id = actor.getActorID()
        actor_name = actor.getActorName()
        age = actor.getAge()
        height = actor.getHeight()
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Actors(actor_id, actor_name, age, height) VALUES({id}, {name}, {a}, {h})").format(
            id=sql.Literal(actor_id), name=sql.Literal(actor_name), a=sql.Literal(age), h=sql.Literal(height))
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
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Actors WHERE actor_id={id}").format(id=sql.Literal(actor_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    result = ResultSet()
    rows_in_output = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Actors WHERE actor_id={id}").format(id=sql.Literal(actor_id))
        rows_in_output, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return CreateActorFromResultSet(result, rows_in_output)


def addMovie(movie: Movie) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        movie_name = movie.getMovieName()
        year = movie.getYear()
        genre = movie.getGenre()
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Movies(movie_name, year, genre) VALUES({name}, {y}, {g})").format(
            name=sql.Literal(movie_name), y=sql.Literal(year), g=sql.Literal(genre))
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
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Movies WHERE movie_name={name} AND year={y}").format(name=sql.Literal(movie_name),
                                                                                          y=sql.Literal(year))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    result = ResultSet()
    rows_in_output = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Movies WHERE movie_name={name} AND year={y}").format(
            name=sql.Literal(movie_name), y=sql.Literal(year))
        rows_in_output, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return CreateMovieFromResultSet(result, rows_in_output)


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        studio_id = studio.getStudioID()
        studio_name = studio.getStudioName()
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Studios(studio_id, studio_name) VALUES({id}, {name})").format(
            id=sql.Literal(studio_id), name=sql.Literal(studio_name))
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
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def deleteStudio(studio_id: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Studios WHERE studio_id={id}").format(id=sql.Literal(studio_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    result = ResultSet()
    rows_in_output = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Studios WHERE studio_id={id}").format(id=sql.Literal(studio_id))
        rows_in_output, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return CreateStudioFromResultSet(result,rows_in_output)


# -----------------------------------------Basic API--------------------------------------------------------
def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Rated(movie_name,year,critic_id,rating) VALUES({name},{year},{id},{rate})").format(
            name=sql.Literal(movieName), year=sql.Literal(movieYear), id=sql.Literal(criticID),
            rate=sql.Literal(rating))
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
        res = ReturnValue.ERROR
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
        res = ReturnValue.ERROR
        print(e)
    finally:
        conn.close()
        return res


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    num_roles = len(roles)
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO PlayedIn(actor_id,movie_name,year,salary,num_roles) Values({id},{name},{year},{s},{n_r});").format(
            id=sql.Literal(actorID), name=sql.Literal(movieName), year=sql.Literal(movieYear), s=sql.Literal(salary),
            n_r=sql.Literal(num_roles))
        for i, r in enumerate(roles):
            if i == 0:
                query += sql.SQL("INSERT INTO PlayedInRole(actor_id,movie_name,year,actor_role) Values")
            query += sql.SQL("({id},{name},{year},{role})").format(id=sql.Literal(actorID), name=sql.Literal(movieName),
                                                                   year=sql.Literal(movieYear), role=sql.Literal(r))
            if i < num_roles - 1:
                query += sql.SQL(",")
        conn.execute(query)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        #print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        #print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        #print(e)
        res = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        #print(e)
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        #print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        #print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()
        return res


def actorDidntPlayInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
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
        #print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        #print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()
        return res

def getActorsRoleInMovie(actor_id: int, movie_name: str, movieYear: int):
    conn = None
    result = ResultSet()
    rows_in_output = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("select actor_role\
                        from playedinrole\
                        where actor_id={id} AND movie_name={name} and year={y} \
                        ORDER BY actor_role DESC").format(id=sql.Literal(actor_id), name=sql.Literal(movie_name),
                                                          y=sql.Literal(movieYear))
        rows_in_output, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ConvertResultSetToList(result, rows_in_output)


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Produced(movie_name,year,studio_id,budget,revenue) VALUES({name},{year},{id},{bud},{rev})").format(
            name=sql.Literal(movieName), year=sql.Literal(movieYear), id=sql.Literal(studioID),
            bud=sql.Literal(budget), rev=sql.Literal(revenue))
        conn.execute(query)
    except DatabaseException.CHECK_VIOLATION as e:
        #print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        #print(e)
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        #print(e)
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        #print(e)
        res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        #print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        #print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()
        return res


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Produced WHERE movie_name={name} AND year={y} AND studio_id={id}").format(
            name=sql.Literal(movieName), y=sql.Literal(movieYear), id=sql.Literal(studioID))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        #print(e)
        res = ReturnValue.ERROR
    except Exception as e:
        #print(e)
        res = ReturnValue.ERROR
    finally:
        conn.close()
        return res


def averageRating(movieName: str, movieYear: int) -> float:
    conn = None
    result = ResultSet()
    res = float(0)
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT (average)::FLOAT FROM movie_avg_rating where movie_name={name} And year={y}").format(
            name=sql.Literal(movieName), y=sql.Literal(movieYear))
        rows, result = conn.execute(query)
        if rows > 0:
            res = result.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def averageActorRating(actorID: int) -> float:
    conn = None
    result = ResultSet()
    res = float(0)
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT (COALESCE(AVG(average),0))::FLOAT AS avg_rating FROM actor_movie_avg_rating where actor_id={id}").format(id=sql.Literal(actorID))
        rows_affected, result = conn.execute(query)
        if rows_affected > 0:
            res = result.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    result = ResultSet()
    rows_in_output = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT ac.movie_name, ac.year , mo.genre \
                        FROM actor_movie_avg_rating ac LEFT JOIN movies mo ON ac.movie_name = mo.movie_name and ac.year = mo.year \
                        where actor_id={id} \
                        ORDER BY average desc, year ASC, movie_name DESC LIMIT 1").format(id=sql.Literal(actor_id))
        rows_in_output, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return CreateMovieFromResultSet(result, rows_in_output)


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    conn = None
    rows, result = 0, ResultSet()
    res = -1
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT q.budget - SUM(COALESCE (pl.salary,0)) as diff\
                        FROM (\
                        SELECT s.movie_name, s.year , COALESCE (pr.budget, 0) as budget \
                        from movies s LEFT JOIN produced pr ON s.movie_name = pr.movie_name AND s.year = pr.year \
                        WHERE s.movie_name = {name} and s.year = {y} \
                        )q LEFT JOIN playedin pl on q.movie_name = pl.movie_name AND q.year = pl.year\
                        GROUP BY q.movie_name, q.year, q.budget").format(name=sql.Literal(movieName), y=sql.Literal(movieYear))
        rows, result = conn.execute(query)
        if rows > 0:
            res = result.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    conn = None
    result = ResultSet()
    res = False
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * \
        FROM(\
        SELECT (num_roles::FLOAT)/total as res\
        FROM (\
        SELECT movie_name, year, SUM(num_roles) as total\
        from playedin\
        WHERE movie_name = {name} and year = {y}\
        GROUP BY movie_name, year\
        ) aa join (\
        SELECT movie_name, year, actor_id, num_roles\
        FROM playedin\
        WHERE movie_name = {name} and year = {y} AND actor_id = {id}\
        ) bb on aa.movie_name = bb.movie_name And aa.year = bb.year\
        ) as q\
        WHERE res >= 0.5").format(name=sql.Literal(movie_name), y=sql.Literal(movie_year), id=sql.Literal(actor_id))
        _, result = conn.execute(query)
        if len(result.rows) > 0:
            res = True
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return res


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT movie_name_from_movies as movie_name, COALESCE(total_rev, 0) as revenue\
                        FROM (\
                        (SELECT DISTINCT movie_name as movie_name_from_movies from movies) A\
                        LEFT JOIN\
                        (SELECT movie_name, SUM(revenue) as total_rev\
                        FROM produced\
                        GROUP by movie_name) B\
                        ON A.movie_name_from_movies = B.movie_name\
                        )\
                        ORDER BY movie_name DESC")
        _, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result.rows


def studioRevenueByYear() -> List[Tuple[int, int, int]]:
    conn = None
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT studio_id, year, SUM(revenue) AS total_revenue\
                        FROM produced\
                        GROUP BY studio_id, year\
                        ORDER BY studio_id DESC, year DESC")
        _, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result.rows


def getFanCritics() -> List[Tuple[int, int]]:
    conn = None
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        # NOTE - this query was not chosen because we believe the other one is more efficient (stated in the dry part)
        # query = sql.SQL("SELECT  C.critic_id,S.studio_id \
        #                         FROM critics C, studios S \
        #                         WHERE EXISTS(SELECT * FROM rated ra WHERE C.critic_id = ra.critic_id) AND EXISTS(SELECT * FROM produced pa WHERE S.studio_id = pa.studio_id) AND not EXISTS( \
        #                             SELECT p.movie_name, P.year \
        #                             FROM produced P \
        #                             WHERE P.studio_id = S.studio_id and ( \
        #                                 (p.movie_name, p.year) NOT IN( \
        #                                 SELECT r.movie_name, r.year \
        #                                 FROM rated r \
        #                                 WHERE r.critic_id = C.critic_id)) \
        #                         ) \
        #                         ORDER BY C.critic_id DESC, S.studio_id DESC")
        query = sql.SQL("SELECT critic_id,q1.studio_id\
                        from (\
                        SELECT r.critic_id, studio_id, COUNT(studio_id) as num_reviews\
                        FROM rated r JOIN produced p\
                        ON r.movie_name=p.movie_name AND r.year=p.year\
                        GROUP BY r.critic_id,p.studio_id\
                        ) AS q1 JOIN (\
                        SELECT studio_id, COUNT(studio_id) as num_movies\
                        FROM produced p\
                        GROUP BY p.studio_id\
                        ) as q2\
                        on q1.studio_id = q2.studio_id AND q1.num_reviews = q2.num_movies\
                        Order BY critic_id DESC, studio_id DESC")
        _, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result.rows


def averageAgeByGenre() -> List[Tuple[str, float]]:
    conn = None
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT genre, (AVG(age)::FLOAT)\
                        FROM (\
                        SELECT DISTINCT genre, age, actor_id\
                        FROM (\
                        SELECT q1.actor_id, q1.movie_name, q1.year, q2.age\
                        FROM (\
                            (\
                            SELECT actor_id, movie_name, year\
                            FROM playedin\
                            ) as q1\
                        JOIN\
                            (\
                            SELECT age, actor_id\
                            FROM actors\
                            ) as q2 ON q1.actor_id=q2.actor_id)\
                        ) as q3 JOIN movies m\
                        ON q3.movie_name=m.movie_name AND q3.year=m.year\
                        ) as q4\
                        GROUP BY genre\
                        ORDER BY genre ASC")
        _, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result.rows


def getExclusiveActors() -> List[Tuple[int, int]]:
    conn = None
    result = ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT DISTINCT q2.actor_id, a.studio_id \
                        From ( \
                          SELECT q1.actor_id, q1.num_studios \
                          FROM ( \
                            SELECT actor_id, COUNT(DISTINCT studio_id) AS num_studios \
                            From actor_movie_studio \
                            GROUP By actor_id \
                            ) as q1 \
                            Where num_studios = 1 \
                            )AS q2 \
                        Join ( \
                          SELECT DISTINCT actor_id,studio_id \
                          FROM actor_movie_studio \
                        )AS a ON q2.actor_id=a.actor_id \
                        ORDER BY q2.actor_id DESC")
        _, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result.rows

# GOOD LUCK!
