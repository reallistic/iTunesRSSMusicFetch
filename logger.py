import logging
from logging.handlers import RotatingFileHandler

log = logging.getLogger(__name__)
try:
    logfile = LOGFILE
except:
    logfile = 'iTunesMusicFetch.log'

try:
    maxsize = LOGFILEMAXSIZE
    if maxsize < 500 * 1024:
        maxsize = 500 * 1024 #500kb
except:
    maxsize = 1 * 1024* 1024

try:
    maxfiles = MAXLOGFILES
except:
    maxfiles = 5
    
LOGLEVEL = logging.INFO
log.setLevel(LOGLEVEL)

#file logger
lh = RotatingFileHandler(logfile, maxBytes=maxsize, backupCount=maxfiles)
lh.setLevel(LOGLEVEL)
fmt = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', '%m/%d/%Y %I:%M:%S')
lh.setFormatter(fmt)
log.addHandler(lh)

#Console logger
ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL)
ch.setFormatter(fmt)
log.addHandler(ch)

log.info("Logging loaded")