__author__ = 'poltak'

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

SPEED_UPPER_LIMIT = 85
SPEED_LOWER_LIMIT = 0
SOCKET_ADDRESS = "localhost"
SOCKET_PORT = 9999


def init_spark(name):
    sc = SparkContext("local[2]", name)
    return StreamingContext(sc, 2)


def close_spark(ssc):
    ssc.start()
    ssc.awaitTermination()


def check_speed(speed):
    if speed > SPEED_UPPER_LIMIT or speed < SPEED_LOWER_LIMIT:
        return speed, False
    else:
        return speed, True


def main():
    ssc = init_spark("pyspark-speed-check")
    speed_stream = ssc.socketTextStream(SOCKET_ADDRESS, SOCKET_PORT)

    speed_stream = speed_stream.map(check_speed)

    speed_stream.pprint()

    close_spark(ssc)


if __name__ == '__main__':
    main()


