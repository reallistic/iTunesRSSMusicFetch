import requests, json, sqlite3, os
import feedparser, datetime, time
import re, sys, logging
reload(sys)
sys.setdefaultencoding("utf-8")
import unicodedata

DBFILE = ''
ITUNESRSS = 'https://itunes.apple.com/us/rss/topalbums/limit=100/explicit=true/json'
SEARCHPROVIDERS = {'Kickass.to':'http://kickass.to/usearch/category:music %s/?rss=1'}
MAXTORRENTSIZE = 300*1024*1024 #300MB
MINTORRENTSIZE = 3*1024*1024 #30MB
RUNEVERYHOURS = 6 #6 hours
MINSEEDS = 2
VERIFIEDTORRENTSONLY =0
TORRENTDIR = ''
MAXSEARCHES = 10
#Do not search with various artist appened
REMOVEVARIOUSARTISTS = True
REPLACEFORSEARCH = '[!@#$\'\\\"\(\)\-\+\,\*]'

LOGFILE = ''
#bytes
#LOGFILEMAXSIZE 
MAXLOGFILES = 3
from logger import log


class ItunesLocalDB:
    def __init__(self, dbfile = ""):
        if dbfile:
            self.dbfile = dbfile
        else:
            self.dbfile = 'iTunesFetchDB.db'
        if not os.path.isfile(self.dbfile):
            log.debug("Creating db")
            self.createDB()
        else:
            log.debug("Loading db")
            self.db = sqlite3.connect(self.dbfile)
            self.db.close()

    def createDB(self):
        try:
            self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES)
            c = self.db.cursor()
            c.execute('''CREATE TABLE titles
                        (titleid INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, artist TEXT, name TEXT,
                            year INTEGER, itunesid INTEGER, wanted INTEGER, snatched INTEGER, searches INTEGER,
                        UNIQUE(itunesid))''')
            c.execute('''CREATE TABLE torrents
                        (id PRIMARY KEY, titleid INTEGER, provider TEXT, uri TEXT, FOREIGN KEY(titleid) REFERENCES titles(titleid))''')
            c.execute('''CREATE TABLE last_search (last_search timestamp)''')
            c.execute('''CREATE TABLE last_feedpull (last_feedpull timestamp)''')
            self.db.commit()
            self.db.close()
        except Exception, e:
            log.error("Error creating database %s" % e)
            self.rebuildDB()

    def execute(self, sql, params = None):
        self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES)
        c = self.db.cursor()
        if params:
            c.execute(sql, params)
        else:
            c.execute(sql)
        self.db.commit()
        self.db.close()

    def query(self, sql, params = None):
        self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES)
        c = self.db.cursor()
        if params:
            c.execute(sql, params)
        else:
            c.execute(sql)
        data = c.fetchall()
        self.db.close()

        return data

    def queryOne(self, sql, params = None):
        self.db = sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES)
        c = self.db.cursor()
        if params:
            c.execute(sql, params)
        else:
            c.execute(sql)
        data = c.fetchone()
        self.db.close()

        return data
    
    def getConnection(self):
        return sqlite3.connect(self.dbfile, detect_types=sqlite3.PARSE_DECLTYPES)

    def rebuildDB(self):
        if os.path.isfile(self.dbfile):
            log.info("Rebuilding db")
            self.db.close()
            os.remove(self.dbfile)
            

class MusicFetch:
    includeTitle = None
    db = None
    
    def __init__(self, args, includeTitle = None):
        log.info('Initiating iTunesFetch with args %s' % " ".join(args))
        force = False
        if "force" in args:
            log.info("Forcing first update")
            force = True

        self.includeTitle = includeTitle
        
        self.db = ItunesLocalDB()
        if "cleardb" in args:
            self.db.rebuildDB()
            self.db = ItunesLocalDB()
        if "clearlog" in args:
            log.handlers[0].doRollover()

        while True:
            self.getTodaysTop(force)
            log.info("Finished searching wanted list. Relaxing for %s hours" % RUNEVERYHOURS)
            time.sleep(RUNEVERYHOURS * 60 * 60) #6 hours

    def getTodaysTop(self, force):
        log.info("Checking if we need to do a feed pull")
        lfp = self.db.queryOne("SELECT last_feedpull from last_feedpull")
        if not lfp or ((datetime.datetime.now() - lfp[0]) > datetime.timedelta(hours = RUNEVERYHOURS)) or force:
            force = False
            if lfp:
                self.db.execute("UPDATE last_feedpull SET last_feedpull = ?", (datetime.datetime.now(),))
            else:
                self.db.execute("INSERT INTO last_feedpull VALUES (?)", (datetime.datetime.now(),))
            log.debug("Requesting itunes top from %s" % ITUNESRSS)
            r = requests.get(ITUNESRSS)
            itunesrss = r.json()
            self.parseRSStoDB(itunesrss)
        else:
            log.info("No update needed")

        self.searchForTitles()

    def parseRSStoDB(self, rss):
        jsonroot = rss["feed"]["entry"]

        for title in jsonroot:
            itunesid = title["id"]["attributes"]["im:id"]
            itunestitle = remove_accents(title["title"]["label"])
            itunesartist = remove_accents(title["im:artist"]["label"])
            itunesname = remove_accents(title["im:name"]["label"])
            itunesyear = title["im:releaseDate"]["attributes"]["label"]
            t = time.strptime(itunesyear,"%B %d, %Y")
            itunesyear = t.tm_year

            if self.includeTitle and not self.includeTitle(title):
                continue
            
            exists = self.db.queryOne("SELECT itunesid FROM titles WHERE itunesid = ?", (itunesid,))
            if not exists:
                log.info("Adding title to the db %s %s" % (itunestitle, itunesyear))
                self.db.execute('''INSERT INTO titles (title, artist, name, year, itunesid, wanted, snatched, searches)
                                    VALUES (?,?,?,?,?,1,0,0)''', (itunestitle, itunesartist, itunesname, itunesyear, itunesid))

    def searchForTitles(self):
        titles = self.db.query("SELECT artist, name, titleid, year FROM titles WHERE wanted = 1 and snatched = 0 and searches < ?",
                                (MAXSEARCHES,))
        for title in titles:
            if REMOVEVARIOUSARTISTS and title[0] == "Various Artists":
                search_string = "%s %s" % (title[1], title[3])
                terms = [title[1], str(title[3])]
            else:
                search_string = "%s %s %s" % (title[0], title[1], title[3])
                terms = [title[0], title[1], str(title[3])]
            log.debug("Title is: %s" % search_string)
            search_string = re.sub(REPLACEFORSEARCH, '', search_string)
            log.debug("Search string is: %s" % search_string)
            for provider,providerurl in SEARCHPROVIDERS.items():
                search_url = providerurl % search_string
                log.debug("Searching for %s in first provider %s" % (search_string, provider))
                d = feedparser.parse(search_url)

                #if len(d) == 0 or len(d["entries"] == 0):
                #    log.info("Nothing found for %s" % search_string)
                matched = False
                tor_link = None
                for entry in d["entries"]:
                    if entry["tags"][0]["term"] == "Music - Mp3" and \
                        int(entry["torrent_contentlength"]) >= MINTORRENTSIZE and \
                        int(entry["torrent_contentlength"]) <= MAXTORRENTSIZE and \
                        int(entry['torrent_seeds']) >= MINSEEDS and \
                        int(entry['torrent_verified']) >= VERIFIEDTORRENTSONLY \
                        and has_words(remove_accents(entry["title"]), terms):

                        gen = (link for link in entry['links'] if link['rel'] == 'enclosure')
                        for link in gen:
                            tor_link = link['href']
                            break
                        if tor_link:
                            log.info("%s is a match for %s at %s" % (entry["title"], search_string, provider))
                            torrentpath = os.path.join(TORRENTDIR, entry['torrent_filename'])
                            log.debug("Downloading torrent file to blackhole dir %s" % torrentpath)
                            r = requests.get(tor_link)
                            try:
                                with open(torrentpath, 'wb') as f:
                                    for chunk in r.iter_content(chunk_size=1024):
                                        f.write(chunk)
                                matched = True
                                self.db.execute("INSERT INTO torrents (titleid, provider, uri) VALUES (?,?,?)",
                                    (title[2],provider,tor_link))
                                self.db.execute("UPDATE titles SET wanted = 0, snatched = 1 WHERE titleid = ?",(title[2],))
                            except Exception, e:
                                log.error("Error downloading torrent file %s to %s" % (entry['torrent_filename'], torrentpath))
                                log.error(e)
                                matched = False
                    if matched:
                        break
            if not matched:
                log.info("No torrent found for %s" % search_string)

            self.db.execute("UPDATE titles SET searches = searches + 1, snatched = 1 WHERE titleid = ?",(title[2],))

def has_words(word, words):
    word = word.lower()
    wordlen = len(word)
    for x in words:
        word = word.replace(x.lower(),'',1)
        if len(word) == wordlen:
            return False
        else:
            wordlen = len(word)
    return True

def remove_accents(input_str):
    nkfd_form = unicodedata.normalize('NFKD', unicode(input_str))
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])

if __name__ == "__main__":
    itunes = MusicFetch(sys.argv)
