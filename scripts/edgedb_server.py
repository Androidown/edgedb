import sys
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.append(ROOT)

from edb.server.main import main


if __name__ == '__main__':
    main()
