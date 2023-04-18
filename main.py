import serial
import pandas as pd
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import time
from df1.models.df1_serial_client import Df1SerialClient

ssh_host = '199.192.18.10'
ssh_username = 'root'
ssh_password = 'Y1UxEavf2F5wp7SN80'
database_username = 'root'
database_password = 'Grafana-7654321'
database_name = 'logger'

connection = None
# localhost = '127.0.0.1'

client = Df1SerialClient(plc_type='SLC 5/04 CPU', src=0x0, dst=0x4,
                         port='COM3',
                         baudrate=19200, parity=serial.PARITY_NONE,
                         stopbits=serial.STOPBITS_ONE, bytesize=serial.EIGHTBITS,
                         timeout=5)
client.connect()


def open_ssh_tunnel(verbose=False):
    """Open an SSH tunnel and connect using a username and password.

    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """

    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=('127.0.0.1', 3306)
    )

    # tunnel.skip_tunnel_checkup = False
    tunnel.start()
    time.sleep(2)
    tunnel.check_tunnels()
    # print(tunnel.tunnel_is_up, flush=True)


open_ssh_tunnel()


# time.sleep(2)


def mysql_connect():
    """Connect to a MySQL server using the SSH tunnel connection

    :return connection: Global MySQL database connection
    """

    global connection

    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port
    )


def run_query(sql):
    """Runs a given SQL query via the global database connection.

    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """

    return pd.read_sql_query(sql, connection)


def exec_query(sql):
    """Runs a given SQL query via the global database connection.

    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """

    with connection.cursor() as cur:
        cur.execute(sql)

    connection.commit()


def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
    """
    tunnel.close()


def mysql_disconnect():
    """Closes the MySQL database connection.
    """
    connection.close()

if (tunnel.tunnel_is_up):
    print(tunnel.tunnel_is_up)
    mysql_connect()


output =0

#eexec_query("create table aseptic_float(time DATETIME NOT NULL, variable VARCHAR(100) NOT NULL, value FLOAT NOT NULL);")
end_time = time.time()

while True:
    #print('Running',i+1)
    start_time = time.time()

    # Reading operations OK
    try:
        if start_time - end_time > 10:
            volume1 = client.read_integer(file_table=53, start=30, total_int=10)
            print(volume1[0]) # Tank 11 volume
            volume2 = client.read_integer(file_table=53, start=31, total_int=10)
            print(volume2[1])  # Tank 12 volume
            volume3 = client.read_integer(file_table=53, start=32, total_int=10)
            print(volume3[2])  # Tank 13 volume
            volume4 = client.read_integer(file_table=53, start=33, total_int=10)
            print(volume4[3])  # Tank 14 volume
            volume5 = client.read_integer(file_table=53, start=34, total_int=10)
            print(volume5[4])  # Tank 15 volume

            #float_arr = client.read_float(file_table=55, start=0, total_float=10)
            #print(float_arr[0]) # Tank 11 temperature


            end_time = time.time()

            exec_query("insert into aseptic_integer (time, variable, value) values (CURRENT_TIMESTAMP, 'tank_11_volume', {});".format(volume1[0]))
            exec_query("insert into aseptic_integer (time, variable, value) values (CURRENT_TIMESTAMP, 'tank_12_volume', {});".format(volume2[1]))
            exec_query("insert into aseptic_integer (time, variable, value) values (CURRENT_TIMESTAMP, 'tank_13_volume', {});".format(volume3[2]))
            exec_query("insert into aseptic_integer (time, variable, value) values (CURRENT_TIMESTAMP, 'tank_14_volume', {});".format(volume4[3]))
            exec_query("insert into aseptic_integer (time, variable, value) values (CURRENT_TIMESTAMP, 'tank_15_volume', {});".format(volume5[4]))
            #exec_query("insert into aseptic_float (time, variable, value) values (CURRENT_TIMESTAMP, 'tank_11_temp', {});".format(float_arr[0]))

        #for j in range(0, 100, 10):
        #    print('N', j, "-",    client.read_integer(file_table=53, start=j, total_int=10)[0])  # Read Integers OK

        #for j in range(0, 100, 10):
          #  print('F', j, "-",    client.read_float(file_table=55, start=j, total_float=10)[0])  # Read Integers OK

        #print('N7:80-2', client.read_integer(start=0, total_int=10))  # Read Integers OK
        #print('Timer4:1',  client.read_timer(start=1, category=TIMER.ACC))  # Read Timer OK
        #print('Counter5:0',client.read_counter(start=0, category=COUNTER.PRE)) # Read Counter OK
        #print('R6:0', client.read_register(start=0, total_int=4))  # Read Registers- CONTROL OK
        #print('B3:0', client.read_binary(start=0))  # Read Binary bits words OK
        #out0 = client.read_output(start=0, bit=BIT.ALL, total_int=1)
        #print('O0:0/1', out0)# , 'bit', client.bit_inspect(out0, BIT.BIT3))  # Read Outputs OK and Bits inspect
        #print('I1:0', client.read_input(start=3, bit=BIT.BIT14, total_int=5))  # Read Inputs
        # # Testing
        #print('Float55:4',client.read_float(start=0, total_float=1))  # Read Float
    except Exception as e:
        print('[ERROR] Runtime error has happened',e)
        client.reconnect()
    except KeyboardInterrupt:
        print('Control+C')
        break

    print('Elapsed(s)', start_time - end_time)
    #print('-------------------')


client.close()

print('end testing, total reconnect', client.reconnect_total())