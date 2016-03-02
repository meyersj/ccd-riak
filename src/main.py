import sys

from utils import connect
import loader
import query


def main():
    if (len(sys.argv) not in [2,3]) or (sys.argv[1] not in ['load','query','clear']):
        print 'Error: invalid usage\nUsage: python main.py load|query'
        sys.exit(1)

    client = connect()
    if sys.argv[1] == 'load':
        if len(sys.argv[2]) == 3 and sys.argv[2] == "loopdata":
            loader.load_loopdata(client)
        else:
            loader.load(client)
    if sys.argv[1] == 'query':
        query.run(client)
    if sys.argv[1] == 'clear':
        loader.clear(client)


if __name__ == '__main__':
    main()
