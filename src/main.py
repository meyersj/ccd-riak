import sys

from utils import connect
import loader
import query


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ['load', 'query']:
        print 'Error: invalid usage\nUsage: python main.py load|query'
        sys.exit(1)

    client = connect()
    if sys.argv[1] == 'load':
        loader.load(client)
    if sys.argv[1] == 'query':
        query.run(client)


if __name__ == '__main__':
    main()
