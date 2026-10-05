"""Put the repo root on sys.path so the tests can import the service module,
and make its module-level load_dotenv() a no-op first: this checkout's local
.env holds real credentials, and nothing in the tests may pick them up."""

import os
import sys

import dotenv

dotenv.load_dotenv = lambda *args, **kwargs: False

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
