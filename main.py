import psycopg2



def create():
    """ create tables in the PostgreSQL database"""

    createCommands = (
        """
        CREATE TABLE Product(prod_id VARCHAR(10), pname VARCHAR(30), price  DECIMAL)
        """,
        """
        ALTER TABLE Product ADD CONSTRAINT pk_product PRIMARY KEY (prod_id)
        """,
        """
        ALTER TABLE Product ADD CONSTRAINT ck_product_price CHECK (price > 0)
        """,

        """
        CREATE TABLE Depot( dep_id  VARCHAR(10), addr     VARCHAR(30), price NUMERIC)
        """,


        """
        ALTER TABLE Depot ADD CONSTRAINT pk_depot PRIMARY KEY (dep_id)
        """,
        
        """
        CREATE TABLE Stock(prod_id VARCHAR(10), dep_id VARCHAR(10),  price NUMERIC)
        """,


        """
        ALTER TABLE Stock ADD CONSTRAINT fk_stock_prod_id FOREIGN KEY(prod_id) REFERENCES Product(prod_id)
        """,
        """
        ALTER TABLE Stock ADD CONSTRAINT fk_stock_dep_id FOREIGN KEY(dep_id) REFERENCES Depot(dep_id)
        
        """)


    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres")

        cur = conn.cursor()
        # create table one by one
        for command in createCommands:
            a = cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()






def insertProduct(*products):
 
    sql = "INSERT INTO Product(prod_id, pname, price) VALUES(%s, %s, %s)"
    conn = None
    try:

        # connect to the PostgreSQL database
        conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres")
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,products)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()




def insertDepot(*depots):
 
    sql = "INSERT INTO Depot(dep_id, addr, price) VALUES(%s, %s, %s)"
    conn = None
    try:

        # connect to the PostgreSQL database
        conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres")
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,depots)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertStock(*stocks):
 
    sql = "INSERT INTO Stock(prod_id, dep_id, price) VALUES(%s, %s, %s)"
    conn = None
    try:

        # connect to the PostgreSQL database
        conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres")
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,stocks)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()




# This function changes the value of d1 to dd1 in all the places as assigned to group 4
def updateDepotAndStock(*depStock):
 
    sql = "BEGIN;ALTER TABLE Stock DROP CONSTRAINT fk_stock_prod_id;ALTER TABLE Stock DROP CONSTRAINT fk_stock_dep_id; ALTER TABLE Stock ADD CONSTRAINT fk_stock_prod_id FOREIGN KEY(prod_id) REFERENCES Product(prod_id) ON UPDATE CASCADE; ALTER TABLE Stock ADD CONSTRAINT fk_stock_dep_id FOREIGN KEY(dep_id) REFERENCES Depot(dep_id) ON UPDATE CASCADE; UPDATE Depot SET dep_id = %s WHERE dep_id =%s; COMMIT;"

    conn = None
    try:

        # connect to the PostgreSQL database
        conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres")
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql,depStock)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def printTables():
     # connect to the PostgreSQL database
        conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                password="postgres")
        # create a new cursor
        cursor = conn.cursor()
        cursor.execute("""SELECT * FROM Product""")

        for table in cursor.fetchall():
            print(table)

        cursor.execute("""SELECT * FROM Depot""")

        for table in cursor.fetchall():
            print(table)

        cursor.execute("""SELECT * FROM Stock""")

        for table in cursor.fetchall():
            print(table)



if __name__ == '__main__':
    try:
        create()

        insertProduct(['p1', 'tape', 2.5])
        insertProduct(['p2', 'tv', 250])
        insertProduct(['p3', 'vcr', 80])

        insertDepot(['d1', 'New York', 9000])
        insertDepot(['d2', 'Syracuse', 6000])
        insertDepot(['d4', 'New York', 2000])

        insertStock(['p1', 'd1', 1000])
        insertStock(['p1', 'd2', -100])
        insertStock(['p1', 'd4', 1200])
        insertStock(['p3', 'd1', 3000])
        insertStock(['p3', 'd4', 2000])
        insertStock(['p2', 'd4', 1500])
        insertStock(['p2', 'd1', -400])
        insertStock(['p2', 'd2', 2000])

        printTables()
        print("\n\n\nChanging the values and printing again\n\n\n")
        updateDepotAndStock(['dd1', 'd1'])
        printTables()




    except (Exception) as error:
        print("There was an error", error)
