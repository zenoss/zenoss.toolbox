import logging
import sys


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s')
log = logging.getLogger(__name__)
