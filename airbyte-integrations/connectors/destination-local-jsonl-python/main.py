import sys
from destination_local_jsonl import DestinationLocalJsonL

if __name__ == "__main__":
    destination = DestinationLocalJsonL()
    destination.run(sys.argv[1:])
